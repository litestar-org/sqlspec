"""OracleDB adapter compiled helpers."""

import atexit
import contextlib
import hashlib
import io
import os
import re
import shutil
import ssl
import stat
import tempfile
import threading
import zipfile
from collections.abc import Callable, Iterable, Mapping, Sequence, Sized
from functools import partial
from pathlib import Path
from typing import TYPE_CHECKING, Any, Final, Protocol, cast

from sqlspec.adapters.oracledb._param_types import OracleBlob, OracleClob, OracleJson, is_json_payload
from sqlspec.adapters.oracledb._typing import DB_TYPE_BLOB, DB_TYPE_CLOB, oracledb_module
from sqlspec.core import (
    DriverParameterProfile,
    ParameterStyle,
    StackResult,
    StatementConfig,
    build_statement_config_from_profile,
    create_sql_result,
)
from sqlspec.core.sqlcommenter import SQLCommenterContext
from sqlspec.data_dictionary.dialects.oracle import (
    ORACLE_JSON_STORAGE_NATIVE,
    parse_oracle_version_components,
    resolve_oracle_json_storage,
)
from sqlspec.driver import rows_to_dicts
from sqlspec.exceptions import (
    CheckViolationError,
    ConnectionTimeoutError,
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
    TransactionError,
    UniqueViolationError,
)
from sqlspec.observability import get_trace_context, get_w3c_trace_context
from sqlspec.typing import NUMPY_INSTALLED
from sqlspec.utils.correlation import CorrelationContext
from sqlspec.utils.module_loader import import_optional
from sqlspec.utils.serializers import from_json, to_json
from sqlspec.utils.text import quote_identifier
from sqlspec.utils.type_converters import build_uuid_coercions
from sqlspec.utils.type_guards import has_rowcount, is_async_readable, is_readable


class OracleVersionCache:
    """Pool-scoped cache for the resolved Oracle server version.

    One instance is created per config and shared with every driver spawned from
    that config's pool, so the server version is resolved once per pool lifetime
    rather than once per acquired session. The cache lives and dies with the
    pool; a server upgrade under a live pool is out of scope and picked up on the
    next pool restart.
    """

    __slots__ = ("resolved", "version")

    def __init__(self) -> None:
        self.resolved: bool = False
        self.version: Any | None = None

    def reset(self) -> None:
        """Clear the cached version so the next resolution re-queries the server."""
        self.resolved = False
        self.version = None


def resolve_oracle_connection_major(connection: Any, version_cache: "OracleVersionCache | None" = None) -> "int | None":
    """Resolve an Oracle server major without issuing a metadata query.

    Pool-scoped data-dictionary metadata is authoritative when it has already
    been resolved. Otherwise, use the connection callback cache and finally
    python-oracledb's connection ``version`` string. The final fallback matters
    for reacquired pool wrappers that do not retain dynamic attributes.
    """
    if version_cache is not None and version_cache.resolved and version_cache.version is not None:
        return int(version_cache.version.major)
    cached_major = getattr(connection, "_sqlspec_oracle_major", None)
    if isinstance(cached_major, int) and not isinstance(cached_major, bool):
        return cached_major
    version = getattr(connection, "version", None)
    if not version:
        return None
    components = parse_oracle_version_components(str(version))
    return components[0] if components is not None else None


def resolve_oracle_connection_compatible_major(
    connection: Any, version_cache: "OracleVersionCache | None" = None
) -> "int | None":
    """Resolve an Oracle server compatible major version without issuing a metadata query."""
    if version_cache is not None and version_cache.resolved and version_cache.version is not None:
        comp = version_cache.version.compatible_major
        return int(comp) if comp is not None else None
    cached_compatible = getattr(connection, "_sqlspec_oracle_compatible_major", None)
    if isinstance(cached_compatible, int) and not isinstance(cached_compatible, bool):
        return cached_compatible
    return None


def chain_input_handler(inner: Any, fallback: "Any | None") -> Any:
    """Build an input type handler that chains ``inner`` to ``fallback``."""
    if fallback is not None and _has_handler(fallback, inner, _chained_input_handler):
        return fallback
    return partial(_chained_input_handler, inner, fallback)


def chain_output_handler(inner: Any, fallback: "Any | None") -> Any:
    """Build an output type handler that chains ``inner`` to ``fallback``."""
    if fallback is not None and _has_handler(fallback, inner, _chained_output_handler):
        return fallback
    return partial(_chained_output_handler, inner, fallback)


def _has_handler(handler: Any, target_inner: Any, chain_function: Any) -> bool:
    current = handler
    while current is not None:
        if current is target_inner:
            return True
        if not isinstance(current, partial) or current.func is not chain_function:
            return False
        inner, current = current.args
        if inner is target_inner:
            return True
    return False


def _chained_input_handler(inner: Any, fallback: "Any | None", cursor: Any, value: Any, arraysize: int) -> Any:
    """Run ``inner`` input handler, falling back to ``fallback`` when it abstains."""
    result = inner(cursor, value, arraysize)
    if result is not None:
        return result
    if fallback is not None:
        return fallback(cursor, value, arraysize)
    return None


def _chained_output_handler(inner: Any, fallback: "Any | None", cursor: Any, metadata: Any) -> Any:
    """Run ``inner`` output handler, falling back to ``fallback`` when it abstains."""
    result = inner(cursor, metadata)
    if result is not None:
        return result
    if fallback is not None:
        return fallback(cursor, metadata)
    return None


if TYPE_CHECKING:
    from collections.abc import Awaitable

    from sqlspec.adapters.oracledb._typing import (
        OracleAsyncConnection,
        OracleAsyncRawCursor,
        OracleSyncConnection,
        OracleSyncRawCursor,
    )
    from sqlspec.core import SQL
    from sqlspec.driver import AsyncExceptionHandler, SyncExceptionHandler


__all__ = (
    "DB_TYPE_BLOB",
    "DB_TYPE_CLOB",
    "ORACLEDB_SUPPORTS_SPARSE_VECTORS",
    "ORACLEDB_VERSION",
    "ORACLE_ACTION_MAX_BYTES",
    "ORACLE_CLIENT_IDENTIFIER_MAX_BYTES",
    "ORACLE_CLIENT_INFO_MAX_BYTES",
    "ORACLE_DBOP_MAX_BYTES",
    "ORACLE_ECID_MAX_BYTES",
    "ORACLE_MODULE_MAX_BYTES",
    "ORACLE_SESSIONLESS_TXN_ID_MAX_BYTES",
    "SPARSE_VECTOR_MIN_DATABASE_MAJOR",
    "VALID_ORACLE_DRIVER_FEATURE_KEYS",
    "OracleAcquireOptions",
    "OracleAsyncStreamSource",
    "OracleBlob",
    "OracleClob",
    "OracleEndUserSecurityConfig",
    "OracleEndpoint",
    "OracleJson",
    "OracleSessionContextConfig",
    "OracleSyncStreamSource",
    "OracleTopologyConfig",
    "OracleVersionCache",
    "apply_driver_features",
    "apply_end_user_security_checkout",
    "apply_oracle_session_trace_attributes",
    "apply_oracle_transaction_priority",
    "apply_wallet_ssl_context",
    "build_arrow_fetch_kwargs",
    "build_connection_config",
    "build_dbms_cloud_copy_data_sql",
    "build_dbms_cloud_format_json",
    "build_duality_etag_update_sql",
    "build_fetch_kwargs",
    "build_insert_statement",
    "build_pipeline_stack_result",
    "build_profile",
    "build_session_checkout_plsql",
    "build_session_context_plsql",
    "build_session_release_plsql",
    "build_session_reset_plsql",
    "build_statement_config",
    "build_tns_description",
    "build_truncate_statement",
    "build_wallet_ssl_context_if_needed",
    "chain_input_handler",
    "chain_output_handler",
    "clear_end_user_security_release",
    "clear_oracle_otel_trace_context",
    "coerce_large_parameters_async",
    "coerce_large_parameters_sync",
    "coerce_many_parameters_async",
    "coerce_many_parameters_sync",
    "coerce_pipeline_rows_async",
    "collect_async_rows",
    "collect_sync_rows",
    "connection_is_thin",
    "create_mapped_exception",
    "default_statement_config",
    "driver_profile",
    "execute_session_checkout_async",
    "execute_session_checkout_sync",
    "execute_session_release_async",
    "execute_session_release_sync",
    "extract_duality_etag",
    "extract_duality_view_etag",
    "extract_oracle_wallet_zip",
    "extract_pipeline_single_row",
    "extract_static_alter_session_config",
    "forget_oracle_session_state",
    "has_callable_alter_session_fields",
    "inject_duality_view_etag",
    "is_stateful_oracle_session_context",
    "normalize_column_names",
    "normalize_execute_many_parameters_async",
    "normalize_execute_many_parameters_sync",
    "normalize_oracle_appcontext",
    "normalize_oracle_config_dsn",
    "normalize_oracle_purity",
    "normalize_oracle_sharding_keys",
    "normalize_oracle_transaction_priority",
    "parse_sqlnet_ora",
    "parse_tnsnames_ora",
    "prepare_session_checkout_pipeline",
    "prepare_session_release_pipeline",
    "read_oracle_lob_async",
    "read_oracle_lob_sync",
    "release_extracted_wallet_dir",
    "resolve_dbms_cloud_source_uri",
    "resolve_oracle_connection_compatible_major",
    "resolve_oracle_connection_major",
    "resolve_row_metadata",
    "resolve_rowcount",
    "retain_extracted_wallet_dir",
    "sanitize_oracle_text_query",
    "supports_df_batches",
    "supports_direct_path_load",
    "supports_opentelemetry_tracing",
    "synthesize_session_state_tag",
    "truncate_utf8_bytes",
    "validate_oracle_drcp_and_mode_constraints",
    "validate_oracle_session_callback",
    "validate_sessionless_execution_options",
    "validate_sessionless_transaction_args",
)


IMPLICIT_UPPER_COLUMN_PATTERN: "re.Pattern[str]" = re.compile(r"^(?!\d)(?:[A-Z0-9_]+)$")
_UNQUOTED_ORACLE_IDENTIFIER_PATTERN: "re.Pattern[str]" = re.compile(r"^[A-Za-z][A-Za-z0-9_$#]*$")
_ORACLE_RESERVED_COLUMN_NAMES: "frozenset[str]" = frozenset({
    "ACCESS",
    "ADD",
    "ALL",
    "ALTER",
    "AND",
    "ANY",
    "AS",
    "ASC",
    "AUDIT",
    "BETWEEN",
    "BY",
    "CHAR",
    "CHECK",
    "CLUSTER",
    "COLUMN",
    "COMMENT",
    "COMPRESS",
    "CONNECT",
    "CREATE",
    "CURRENT",
    "DATE",
    "DECIMAL",
    "DEFAULT",
    "DELETE",
    "DESC",
    "DISTINCT",
    "DROP",
    "ELSE",
    "EXCLUSIVE",
    "EXISTS",
    "FILE",
    "FLOAT",
    "FOR",
    "FROM",
    "GRANT",
    "GROUP",
    "HAVING",
    "IDENTIFIED",
    "IMMEDIATE",
    "IN",
    "INCREMENT",
    "INDEX",
    "INITIAL",
    "INSERT",
    "INTEGER",
    "INTERSECT",
    "INTO",
    "IS",
    "LEVEL",
    "LIKE",
    "LOCK",
    "LONG",
    "MAXEXTENTS",
    "MINUS",
    "MLSLABEL",
    "MODE",
    "MODIFY",
    "NOAUDIT",
    "NOCOMPRESS",
    "NOT",
    "NOWAIT",
    "NULL",
    "NUMBER",
    "OF",
    "OFFLINE",
    "ON",
    "ONLINE",
    "OPTION",
    "OR",
    "ORDER",
    "PCTFREE",
    "PRIOR",
    "PUBLIC",
    "RAW",
    "RENAME",
    "RESOURCE",
    "REVOKE",
    "ROW",
    "ROWID",
    "ROWNUM",
    "ROWS",
    "SELECT",
    "SESSION",
    "SET",
    "SHARE",
    "SIZE",
    "SMALLINT",
    "START",
    "SUCCESSFUL",
    "SYNONYM",
    "SYSDATE",
    "TABLE",
    "THEN",
    "TO",
    "TRIGGER",
    "UID",
    "UNION",
    "UNIQUE",
    "UPDATE",
    "USER",
    "VALIDATE",
    "VALUES",
    "VARCHAR",
    "VARCHAR2",
    "VIEW",
    "WHENEVER",
    "WHERE",
    "WITH",
})
_VERSION_COMPONENTS: int = 3
_LOB_TYPE_NAME_MARKERS: "tuple[str, ...]" = ("LOB", "BFILE")
_SCALAR_PASSTHROUGH_TYPES: "tuple[type[Any], ...]" = (bool, int, float, str, bytes, bytearray, type(None))
_BIND_PASSTHROUGH_TYPES: "tuple[type[Any], ...]" = (bool, int, float, type(None))
ROW_CACHE_MAX_SIZE: int = 256

ORA_CHECK_CONSTRAINT = 2290
ORA_INTEGRITY_RANGE_START = 2200
ORA_INTEGRITY_RANGE_END = 2300
ORA_PARSING_RANGE_START = 900
ORA_PARSING_RANGE_END = 1000
ORA_TABLESPACE_FULL = 1652

_ERROR_CODE_MAPPING: "dict[int, tuple[type[SQLSpecError], str]]" = {
    1: (UniqueViolationError, "unique constraint violation"),
    28: (DatabaseConnectionError, "session has been killed"),
    54: (OperationalError, "resource busy and acquire with NOWAIT specified or timeout expired"),
    60: (DeadlockError, "deadlock detected"),
    942: (SQLParsingError, "table or view does not exist"),
    1013: (OperationCancelledError, "user requested cancel"),
    1017: (PermissionDeniedError, "invalid username/password"),
    1031: (PermissionDeniedError, "insufficient privileges"),
    1400: (NotNullViolationError, "not-null constraint violation"),
    1407: (NotNullViolationError, "not-null constraint violation"),
    1438: (DataError, "numeric precision overflow"),
    1476: (DataError, "divisor is equal to zero"),
    1652: (OperationalError, "tablespace full"),
    1722: (DataError, "invalid number"),
    1830: (DataError, "date format picture ends before converting entire input string"),
    1840: (DataError, "data conversion error"),
    1843: (DataError, "not a valid month"),
    1858: (DataError, "invalid character"),
    1861: (DataError, "literal does not match format string"),
    2291: (ForeignKeyViolationError, "foreign key constraint violation"),
    2292: (ForeignKeyViolationError, "foreign key constraint violation"),
    ORA_CHECK_CONSTRAINT: (CheckViolationError, "check constraint violation"),
    3113: (DatabaseConnectionError, "end-of-file on communication channel"),
    3114: (DatabaseConnectionError, "not connected to ORACLE"),
    3135: (DatabaseConnectionError, "connection lost contact"),
    3136: (ConnectionTimeoutError, "inbound connection timed out"),
    4068: (OperationalError, "existing state of packages has been discarded"),
    8176: (TransactionError, "consistent read failure"),
    8177: (SerializationConflictError, "cannot serialize access"),
    12154: (DatabaseConnectionError, "TNS resolution failed"),
    12170: (ConnectionTimeoutError, "connect timeout"),
    12505: (DatabaseConnectionError, "listener rejected"),
    12514: (DatabaseConnectionError, "service not known"),
    12541: (ConnectionTimeoutError, "no listener"),
    12545: (ConnectionTimeoutError, "connect failed"),
    12899: (DataError, "value too large for column"),
    24756: (TransactionError, "transaction does not exist"),
    25351: (TransactionError, "transaction is currently in use"),
    26202: (TransactionError, "no sessionless transaction is active"),
    26211: (TransactionError, "sessionless transaction cannot mix DBMS_TRANSACTION and driver methods"),
    26216: (TransactionError, "sessionless transaction is already active"),
    28000: (PermissionDeniedError, "the account is locked"),
    28001: (PermissionDeniedError, "the password has expired"),
    30006: (OperationalError, "resource busy; acquire with WAIT timeout expired"),
    40441: (DataError, "JSON syntax error"),
    42699: (SerializationConflictError, "transaction isolation violation"),
    51803: (DataError, "vector operation error"),
}

_DPY_ERROR_CODE_MAPPING: "dict[str, tuple[type[SQLSpecError], str]]" = {
    "DPY-1001": (DatabaseConnectionError, "not connected to database"),
    "DPY-2056": (ImproperConfigurationError, "centralized config protocol handler failed"),
    "DPY-2057": (ImproperConfigurationError, "password type handler failed"),
    "DPY-2058": (ImproperConfigurationError, "password in centralized configuration must specify a type"),
    "DPY-2061": (ImproperConfigurationError, "registered params hook handler failed"),
    "DPY-2072": (DataError, "end-user security attribute data exceeds 1024-byte protocol limit"),
    "DPY-2073": (ImproperConfigurationError, "end-user security context requires Oracle AI Database 26ai over TCPS"),
    "DPY-3034": (TransactionError, "sessionless transaction cannot mix DBMS_TRANSACTION and driver methods"),
    "DPY-3035": (TransactionError, "sessionless transaction is already active"),
    "DPY-3036": (TransactionError, "no sessionless transaction is active"),
    "DPY-3041": (OperationalError, "database does not support Oracle Deep Data Security"),
    "DPY-3043": (OperationalError, "database does not support transaction priority"),
    "DPY-3044": (OperationalError, "database does not support OpenTelemetry tracing"),
    "DPY-4004": (DataError, "invalid number"),
    "DPY-4005": (ConnectionTimeoutError, "connection pool checkout timed out"),
    "DPY-4021": (ImproperConfigurationError, "invalid connection protocol"),
    "DPY-4027": (ImproperConfigurationError, "no configuration directory specified"),
    "DPY-4033": (DatabaseConnectionError, "connection closed due to exceeded idle time"),
    "DPY-4034": (ImproperConfigurationError, "invalid password type"),
    "DPY-4011": (DatabaseConnectionError, "connection was closed"),
    "DPY-4024": (QueryTimeoutError, "query execution timeout expired"),
    "DPY-6000": (DatabaseConnectionError, "cannot connect to database"),
    "DPY-6001": (DatabaseConnectionError, "cannot connect to database"),
    "DPY-6005": (ConnectionTimeoutError, "cannot connect to database"),
}


SPARSE_VECTOR_MIN_DATABASE_MAJOR: int = 23

ORACLEDB_SUPPORTS_SPARSE_VECTORS: bool = True

VALID_ORACLE_DRIVER_FEATURE_KEYS: Final[frozenset[str]] = frozenset({
    "_statement_cache_holder",
    "on_pool_destroying",
    "on_session_end",
    "on_session_start",
    "params_hooks",
    "plugins",
    "shard_resolver",
    "sharding_key_variable",
    "super_sharding_key_variable",
    "use_tenant_id_as_sharding_key",
    "enable_auto_state_tagging",
    "enable_otel_trace_context",
    "arraysize",
    "arrow_requested_schema",
    "custom",
    "dbms_cloud_credential",
    "enable_direct_path_load",
    "enable_events",
    "enable_lob_fetch",
    "enable_lowercase_column_names",
    "enable_numpy_vectors",
    "enable_session_tracing",
    "enable_uuid_binary",
    "events_backend",
    "fetch_decimals",
    "fetch_lobs",
    "json_deserializer",
    "json_serializer",
    "on_connection_create",
    "on_connection_destroy",
    "on_pool_create",
    "on_pool_destroy",
    "oracle_raw_byte_limit",
    "oracle_varchar2_byte_limit",
    "prefetchrows",
    "session_context_hook",
    "tenant_strategy",
    "storage_capabilities",
    "vector_return_format",
})

_DBMS_CLOUD_FORMAT_MAP: Final[dict[str, str]] = {
    "parquet": "parquet",
    "csv": "csv",
    "json": "json",
    "jsonl": "json",
    "ndjson": "json",
}

_ORACLE_UPPER_SIMPLE_IDENTIFIER_PATTERN: "re.Pattern[str]" = re.compile(r"^[A-Z][A-Z0-9_$#]*$")
_WALLET_DIR_MODE: Final[int] = 0o700
_WALLET_FILE_MODE: Final[int] = 0o600
_MIN_QUOTED_IDENTIFIER_LEN: Final[int] = 2
_QUALIFIED_TABLE_PARTS: Final[int] = 2
_WALLET_DIR_LOCK: Final[threading.Lock] = threading.Lock()
_WALLET_DIR_REFCOUNTS: Final[dict[Path, int]] = {}
_ATEXIT_WALLET_DIRS: Final[set[Path]] = set()
_ATEXIT_WALLET_HOOK_REGISTERED: list[bool] = [False]

_ORACLE_TIME_ZONE_PATTERN: "re.Pattern[str]" = re.compile(r"^[A-Za-z0-9_+\-/:]{1,64}$")
_ORACLE_NLS_KEY_PATTERN: "re.Pattern[str]" = re.compile(r"^NLS_[A-Z0-9_]{1,60}$")
_ORACLE_TEXT_TOKEN_PATTERN: "re.Pattern[str]" = re.compile(r"[A-Za-z0-9_]+")
_ORACLE_TEXT_OPERATORS: Final[frozenset[str]] = frozenset({"ACCUM", "AND", "OR", "NEAR"})
_ORACLE_TEXT_RESERVED_OPERATORS: "frozenset[str]" = frozenset({
    "ABOUT",
    "ACCUM",
    "AND",
    "BT",
    "DEFINEMERGE",
    "DEFINESCORE",
    "EQUIV",
    "FUZZY",
    "HASPATH",
    "INPATH",
    "MDATA",
    "MINUS",
    "NEAR",
    "NOT",
    "NT",
    "OR",
    "PT",
    "RT",
    "SOUNDEX",
    "SQE",
    "STEM",
    "SYN",
    "TR",
    "TRSYN",
    "WEIGHT",
    "WILDCARD",
    "WITHIN",
})


_MAX_TCP_PORT: int = 65535


class OracleEndpoint:
    """Typed Oracle Net listener endpoint for multi-endpoint TNS descriptors."""

    __slots__ = ("host", "port", "protocol")

    def __init__(self, host: str, port: int = 1521, protocol: str = "tcp") -> None:
        if not host or not host.strip():
            msg = "OracleEndpoint host must be non-empty"
            raise ImproperConfigurationError(msg)
        cleaned_host = host.strip()
        _validate_tns_token(cleaned_host, "host")
        if isinstance(port, bool) or not isinstance(port, int) or port <= 0 or port > _MAX_TCP_PORT:
            msg = "OracleEndpoint port must be an integer between 1 and 65535"
            raise ImproperConfigurationError(msg)
        normalized_protocol = protocol.strip().lower()
        if normalized_protocol not in {"tcp", "tcps"}:
            msg = f"OracleEndpoint protocol must be 'tcp' or 'tcps', got {protocol!r}"
            raise ImproperConfigurationError(msg)
        self.host: str = cleaned_host
        self.port: int = port
        self.protocol: str = normalized_protocol

    def __repr__(self) -> str:
        return f"OracleEndpoint(host={self.host!r}, port={self.port!r}, protocol={self.protocol!r})"

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, OracleEndpoint):
            return NotImplemented
        return (self.host, self.port, self.protocol) == (other.host, other.port, other.protocol)

    def __hash__(self) -> int:
        return hash((self.host, self.port, self.protocol))


class OracleTopologyConfig:
    """Typed Oracle Net multi-endpoint topology configuration for RAC, Data Guard, and CMAN."""

    __slots__ = (
        "endpoints",
        "failover",
        "failover_delay",
        "failover_mode_method",
        "failover_mode_type",
        "failover_retries",
        "load_balance",
        "retry_count",
        "retry_delay",
        "sdu",
        "server",
        "service_name",
        "source_route",
        "transport_connect_timeout",
    )

    def __init__(
        self,
        endpoints: "Sequence[OracleEndpoint | Mapping[str, Any]]",
        service_name: str,
        *,
        load_balance: bool | None = True,
        failover: bool | None = True,
        retry_count: int | None = None,
        retry_delay: int | None = None,
        transport_connect_timeout: float | None = None,
        sdu: int | None = None,
        source_route: bool = False,
        server: str | None = None,
        failover_mode_type: str | None = None,
        failover_mode_method: str | None = None,
        failover_type: str | None = None,
        failover_method: str | None = None,
        failover_retries: int | None = None,
        failover_delay: int | None = None,
    ) -> None:
        if not endpoints:
            msg = "OracleTopologyConfig requires at least one endpoint"
            raise ImproperConfigurationError(msg)
        if not service_name or not service_name.strip():
            msg = "OracleTopologyConfig service_name must be non-empty"
            raise ImproperConfigurationError(msg)
        cleaned_service_name = service_name.strip()
        _validate_tns_token(cleaned_service_name, "service_name")
        normalized_endpoints: list[OracleEndpoint] = []
        for ep in endpoints:
            if isinstance(ep, OracleEndpoint):
                normalized_endpoints.append(ep)
            elif isinstance(ep, Mapping):
                normalized_endpoints.append(
                    OracleEndpoint(
                        host=str(ep.get("host", "")),
                        port=int(ep.get("port", 1521)),
                        protocol=str(ep.get("protocol", "tcp")),
                    )
                )
            else:
                msg = f"Invalid endpoint in OracleTopologyConfig: {ep!r}"
                raise ImproperConfigurationError(msg)
        resolved_server = server.strip().upper() if server else None
        if resolved_server is not None:
            _validate_tns_token(resolved_server, "server")
        raw_fm_type = failover_mode_type if failover_mode_type is not None else failover_type
        raw_fm_method = failover_mode_method if failover_mode_method is not None else failover_method
        resolved_fm_type = raw_fm_type.strip().upper() if raw_fm_type else None
        if resolved_fm_type is not None:
            _validate_tns_token(resolved_fm_type, "failover_mode_type")
        resolved_fm_method = raw_fm_method.strip().upper() if raw_fm_method else None
        if resolved_fm_method is not None:
            _validate_tns_token(resolved_fm_method, "failover_mode_method")
        self.endpoints: tuple[OracleEndpoint, ...] = tuple(normalized_endpoints)
        self.service_name: str = cleaned_service_name
        self.load_balance: bool | None = None if load_balance is None else bool(load_balance)
        self.failover: bool | None = None if failover is None else bool(failover)
        self.retry_count: int | None = retry_count
        self.retry_delay: int | None = retry_delay
        self.transport_connect_timeout: float | None = transport_connect_timeout
        self.sdu: int | None = sdu
        self.source_route: bool = bool(source_route)
        self.server: str | None = resolved_server
        self.failover_mode_type: str | None = resolved_fm_type
        self.failover_mode_method: str | None = resolved_fm_method
        self.failover_retries: int | None = failover_retries
        self.failover_delay: int | None = failover_delay

    def __repr__(self) -> str:
        return (
            f"OracleTopologyConfig(endpoints={self.endpoints!r}, service_name={self.service_name!r}, "
            f"load_balance={self.load_balance!r}, failover={self.failover!r})"
        )

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, OracleTopologyConfig):
            return NotImplemented
        return (
            self.endpoints,
            self.service_name,
            self.load_balance,
            self.failover,
            self.retry_count,
            self.retry_delay,
            self.transport_connect_timeout,
            self.sdu,
            self.source_route,
            self.server,
            self.failover_mode_type,
            self.failover_mode_method,
            self.failover_retries,
            self.failover_delay,
        ) == (
            other.endpoints,
            other.service_name,
            other.load_balance,
            other.failover,
            other.retry_count,
            other.retry_delay,
            other.transport_connect_timeout,
            other.sdu,
            other.source_route,
            other.server,
            other.failover_mode_type,
            other.failover_mode_method,
            other.failover_retries,
            other.failover_delay,
        )

    def __hash__(self) -> int:
        return hash((
            self.endpoints,
            self.service_name,
            self.load_balance,
            self.failover,
            self.retry_count,
            self.retry_delay,
            self.transport_connect_timeout,
            self.sdu,
            self.source_route,
            self.server,
            self.failover_mode_type,
            self.failover_mode_method,
            self.failover_retries,
            self.failover_delay,
        ))


class OracleEndUserSecurityConfig:
    """Explicit per-session Oracle Deep Data Security credentials."""

    __slots__ = ("attributes", "data_roles", "database_access_token", "end_user_identity")

    def __init__(
        self,
        database_access_token: str | Callable[[], str],
        *,
        end_user_identity: Any = None,
        data_roles: Any = None,
        attributes: Any = None,
    ) -> None:
        if not callable(database_access_token) and (
            not isinstance(database_access_token, str) or not database_access_token
        ):
            msg = "database_access_token must be a nonempty string or callable"
            raise ImproperConfigurationError(msg)
        if end_user_identity is not None and not callable(end_user_identity):
            _validate_end_user_identity(end_user_identity)
        if data_roles is not None and not callable(data_roles):
            _validate_end_user_roles(data_roles)
        if attributes is not None and not callable(attributes):
            _validate_end_user_attributes(attributes)
        self.database_access_token = database_access_token
        self.end_user_identity = end_user_identity
        self.data_roles = list(data_roles) if data_roles is not None and not callable(data_roles) else data_roles
        self.attributes = dict(attributes) if attributes is not None and not callable(attributes) else attributes

    def __repr__(self) -> str:
        return "OracleEndUserSecurityConfig(database_access_token='***', end_user_identity='***', data_roles=<redacted>, attributes=<redacted>)"

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, OracleEndUserSecurityConfig):
            return NotImplemented
        return (self.database_access_token, self.end_user_identity, self.data_roles, self.attributes) == (
            other.database_access_token,
            other.end_user_identity,
            other.data_roles,
            other.attributes,
        )

    def __hash__(self) -> int:
        roles = tuple(self.data_roles) if isinstance(self.data_roles, Sequence) else self.data_roles
        attributes = tuple(sorted(self.attributes.items())) if isinstance(self.attributes, Mapping) else self.attributes
        return hash((self.database_access_token, self.end_user_identity, roles, attributes))


def _validate_end_user_identity(value: Any) -> None:
    if isinstance(value, str) and value:
        return
    if (
        isinstance(value, tuple)
        and len(value) == _QUALIFIED_TABLE_PARTS
        and isinstance(value[0], str)
        and value[0]
        and (value[1] is None or (isinstance(value[1], str) and value[1]))
    ):
        return
    msg = "end_user_identity must be a nonempty string or (user_name, key) pair"
    raise ImproperConfigurationError(msg)


def _validate_end_user_roles(value: Any) -> None:
    if (
        not isinstance(value, Sequence)
        or isinstance(value, (str, bytes))
        or any(not isinstance(role, str) or not role for role in value)
    ):
        msg = "data_roles must be a sequence of nonempty strings"
        raise ImproperConfigurationError(msg)


def _validate_end_user_attributes(value: Any) -> None:
    if not isinstance(value, Mapping):
        msg = "end-user attributes must be a mapping"
        raise ImproperConfigurationError(msg)
    for key, item in value.items():
        _validate_oracle_identifier(key, "end-user attribute")
        if not isinstance(item, str):
            msg = "end-user attributes must contain string values"
            raise ImproperConfigurationError(msg)


def apply_end_user_security_checkout(connection: Any, config: OracleEndUserSecurityConfig) -> None:
    """Attach explicitly scoped native security credentials without a network call."""
    if not connection_is_thin(connection):
        msg = "Oracle Deep Data Security requires Thin mode over TCPS"
        raise ImproperConfigurationError(msg)
    create_context = getattr(oracledb_module, "create_end_user_security_context", None)
    if create_context is None or not callable(getattr(connection, "set_end_user_security_context", None)):
        msg = "Oracle Deep Data Security requires python-oracledb 26.1 or newer"
        raise oracledb_module.NotSupportedError(msg)
    token = config.database_access_token() if callable(config.database_access_token) else config.database_access_token
    identity = config.end_user_identity() if callable(config.end_user_identity) else config.end_user_identity
    roles = config.data_roles() if callable(config.data_roles) else config.data_roles
    attrs = config.attributes() if callable(config.attributes) else config.attributes
    validated = OracleEndUserSecurityConfig(token, end_user_identity=identity, data_roles=roles, attributes=attrs)
    try:
        context = create_context(
            validated.database_access_token,
            end_user_identity=validated.end_user_identity,
            data_roles=validated.data_roles,
            attributes=validated.attributes,
        )
        connection.set_end_user_security_context(context)
    except oracledb_module.DatabaseError as exc:
        raise create_mapped_exception(exc) from exc


def clear_end_user_security_release(connection: Any) -> None:
    """Unconditionally detach session credentials before pool release."""
    try:
        connection.clear_end_user_security_context()
    except oracledb_module.DatabaseError as exc:
        raise create_mapped_exception(exc) from exc


_INITIAL_SCHEMA_BY_CONN_ID: dict[int, str] = {}
_INITIAL_PRIORITY_BY_CONN_ID: dict[int, Any] = {}


def forget_oracle_session_state(connection: Any) -> None:
    """Remove checkout baselines when a physical connection is discarded."""
    _INITIAL_SCHEMA_BY_CONN_ID.pop(id(connection), None)
    _INITIAL_PRIORITY_BY_CONN_ID.pop(id(connection), None)


def normalize_oracle_appcontext(
    raw_appcontext: Sequence[tuple[str, str, str]] | Mapping[str, Mapping[str, str]],
) -> list[tuple[str, str, str]]:
    """Validate and normalize native connect-time application context triplets."""
    if isinstance(raw_appcontext, Mapping):
        result = [
            (namespace, key, value) for namespace, attrs in raw_appcontext.items() for key, value in attrs.items()
        ]
    elif isinstance(raw_appcontext, Sequence) and not isinstance(raw_appcontext, (str, bytes)):
        result = list(raw_appcontext)
    else:
        msg = "Oracle appcontext must be context triplets or a nested mapping"
        raise ImproperConfigurationError(msg)
    for entry in result:
        if (
            not isinstance(entry, (tuple, list))
            or len(entry) != _VERSION_COMPONENTS
            or any(not isinstance(item, str) for item in entry)
        ):
            msg = "Oracle appcontext entries must contain three strings"
            raise ImproperConfigurationError(msg)
        _validate_oracle_identifier(entry[0], "context namespace")
        _validate_oracle_identifier(entry[1], "context attribute")
    return [(entry[0], entry[1], entry[2]) for entry in result]


def extract_pipeline_single_row(pipeline_result: Any) -> tuple[Any, ...] | None:
    """Extract the first native fetchone result without fabricating a missing row."""
    rows = pipeline_result.rows
    return tuple(rows[0]) if rows else None


def _copy_oracle_session_context(
    config: "OracleSessionContextConfig", *, contexts: Any, edition: Any
) -> "OracleSessionContextConfig":
    return OracleSessionContextConfig(
        contexts=contexts,
        edition=edition,
        module=config.module,
        action=config.action,
        client_identifier=config.client_identifier,
        ecid=config.ecid,
        time_zone=config.time_zone,
        nls_parameters=config.nls_parameters,
        ebs_apps_initialize=config.ebs_apps_initialize,
        mo_global_policy=config.mo_global_policy,
        apex_session=config.apex_session,
        reset_package=config.reset_package,
        clear_identifier=config.clear_identifier,
        clear_context_namespaces=config.clear_context_namespaces,
        apex_detach=config.apex_detach,
        reset_on_release=config.reset_on_release,
        clientinfo=config.clientinfo,
        current_schema=config.current_schema,
        dbop=config.dbop,
        transaction_priority=config.transaction_priority,
        end_user_security=config.end_user_security,
    )


def prepare_session_checkout_pipeline(
    connection: Any, config: "OracleSessionContextConfig | Mapping[str, Any]", *, skip_alter_session: bool = False
) -> tuple[str | None, dict[str, Any]]:
    """Apply zero-round-trip connection fields and prepare optional checkout SQL."""
    resolved = OracleSessionContextConfig(**config) if isinstance(config, Mapping) else config
    if resolved.current_schema is not None:
        baseline = getattr(connection, "current_schema", "") or getattr(connection, "username", "")
        if isinstance(baseline, str):
            _INITIAL_SCHEMA_BY_CONN_ID.setdefault(id(connection), baseline)
    if resolved.transaction_priority is not None:
        if getattr(oracledb_module, "TransactionPriority", None) is None:
            msg = "Oracle transaction priority requires python-oracledb 26.1 or newer"
            raise oracledb_module.NotSupportedError(msg)
        _INITIAL_PRIORITY_BY_CONN_ID.setdefault(id(connection), connection.transaction_priority)
        apply_oracle_transaction_priority(connection, resolved.transaction_priority)
    if resolved.end_user_security is not None:
        apply_end_user_security_checkout(connection, resolved.end_user_security)
    contexts = resolved.contexts() if callable(resolved.contexts) else resolved.contexts
    if callable(resolved.contexts):
        resolved = _copy_oracle_session_context(resolved, contexts=contexts, edition=resolved.edition)
    if contexts:
        for namespace, attrs in contexts.items():
            if namespace.upper() == "CLIENTCONTEXT":
                values: dict[str, str] = {}
                for key, raw in attrs.items():
                    value = raw() if callable(raw) else raw
                    if value is not None:
                        if not isinstance(value, str):
                            msg = "CLIENTCONTEXT values must be strings"
                            raise ImproperConfigurationError(msg)
                        values[key] = value
                connection.set_app_context("CLIENTCONTEXT", **values)
    edition = _resolve_callable_or_str(resolved.edition)
    actual = getattr(connection, "edition", None)
    if edition is not None and isinstance(actual, str) and edition.upper() == actual.upper():
        resolved = _copy_oracle_session_context(resolved, contexts=contexts, edition=None)
    plsql, binds, piggybacks = _build_session_checkout_plan(resolved, skip_alter_session=skip_alter_session)
    for name, value in piggybacks.items():
        if name == "econtext_id" and connection_is_thin(connection):
            continue
        if value is not None:
            maximum = ORACLE_MODULE_MAX_BYTES if name == "module" else ORACLE_ACTION_MAX_BYTES
            value = truncate_utf8_bytes(value, maximum) if name != "current_schema" else value
        with contextlib.suppress(AttributeError):
            setattr(connection, name, value)
    return plsql, binds


def prepare_session_release_pipeline(
    connection: Any, config: "OracleSessionContextConfig | Mapping[str, Any]", *, defer_identifier_reset: bool = False
) -> tuple[str | None, dict[str, Any], Callable[[], None] | None]:
    """Clear zero-round-trip state and prepare bound cleanup SQL."""
    resolved = OracleSessionContextConfig(**config) if isinstance(config, Mapping) else config
    if resolved.end_user_security is not None:
        clear_end_user_security_release(connection)
    contexts = resolved.contexts() if callable(resolved.contexts) else resolved.contexts
    if callable(resolved.contexts):
        resolved = _copy_oracle_session_context(resolved, contexts=contexts, edition=resolved.edition)
    namespaces = set(resolved.clear_context_namespaces)
    if resolved.reset_on_release and contexts:
        namespaces.update(contexts)
    if any(namespace.upper() == "CLIENTCONTEXT" for namespace in namespaces):
        connection.clear_app_context("CLIENTCONTEXT")
    schema = _INITIAL_SCHEMA_BY_CONN_ID.pop(id(connection), None)
    priority = _INITIAL_PRIORITY_BY_CONN_ID.pop(id(connection), None)
    if resolved.reset_on_release:
        if schema is not None:
            connection.current_schema = schema
        if priority is not None:
            apply_oracle_transaction_priority(connection, priority)
    plsql, binds, piggybacks = _build_session_release_plan(resolved)
    deferred = None
    for name, value in piggybacks.items():
        if name == "econtext_id" and connection_is_thin(connection):
            continue
        if name == "client_identifier" and defer_identifier_reset:
            deferred = partial(setattr, connection, name, value)
        else:
            with contextlib.suppress(AttributeError):
                setattr(connection, name, value)
    return plsql, binds, deferred


class OracleSessionContextConfig:
    """Declarative Oracle session context, EBR edition, NLS, EBS, APEX, and VPD configuration."""

    __slots__ = (
        "action",
        "apex_detach",
        "apex_session",
        "clear_context_namespaces",
        "clear_identifier",
        "client_identifier",
        "clientinfo",
        "context_attributes",
        "contexts",
        "current_schema",
        "dbop",
        "ebs_apps_initialize",
        "ecid",
        "edition",
        "end_user_security",
        "mo_global_policy",
        "module",
        "nls_parameters",
        "reset_on_release",
        "reset_package",
        "time_zone",
        "transaction_priority",
    )

    def __init__(
        self,
        *,
        clientinfo: Any = None,
        current_schema: Any = None,
        dbop: Any = None,
        transaction_priority: Any = None,
        end_user_security: OracleEndUserSecurityConfig | Mapping[str, Any] | None = None,
        module: "str | Callable[[], str | None] | None" = None,
        action: "str | Callable[[], str | None] | None" = None,
        client_identifier: "str | Callable[[], str | None] | None" = None,
        ecid: "str | Callable[[], str | None] | None" = None,
        edition: "str | Callable[[], str | None] | None" = None,
        time_zone: "str | Callable[[], str | None] | None" = None,
        nls_parameters: "Mapping[str, str] | Callable[[], Mapping[str, str] | None] | None" = None,
        contexts: "Mapping[str, Mapping[str, str | Callable[[], str | None]]] | Callable[[], Mapping[str, Mapping[str, str]] | None] | None" = None,
        context_attributes: "Mapping[str, Mapping[str, str | Callable[[], str | None]]] | Callable[[], Mapping[str, Mapping[str, str]] | None] | None" = None,
        ebs_apps_initialize: "tuple[int, int, int] | None" = None,
        mo_global_policy: "tuple[str, int | None] | None" = None,
        apex_session: "tuple[int, int, int] | None" = None,
        reset_package: bool = False,
        clear_identifier: bool = False,
        clear_context_namespaces: "Sequence[str]" = (),
        apex_detach: bool = False,
        reset_on_release: bool = True,
    ) -> None:
        if isinstance(edition, str):
            _validate_oracle_identifier(edition, "edition")
        if isinstance(time_zone, str):
            _validate_oracle_time_zone(time_zone)
        if isinstance(nls_parameters, Mapping):
            _validate_oracle_nls_parameters(nls_parameters)
        resolved_contexts = contexts if contexts is not None else context_attributes
        if isinstance(resolved_contexts, Mapping):
            _validate_oracle_contexts(resolved_contexts)
        for ns in clear_context_namespaces:
            _validate_oracle_identifier(ns, "context namespace")
        self.clientinfo = clientinfo
        self.current_schema = current_schema
        self.dbop = dbop
        self.transaction_priority = transaction_priority
        self.end_user_security = (
            OracleEndUserSecurityConfig(**end_user_security)
            if isinstance(end_user_security, Mapping)
            else end_user_security
        )
        self.module = module
        self.action = action
        self.client_identifier = client_identifier
        self.ecid = ecid
        self.edition = edition
        self.time_zone = time_zone
        self.nls_parameters = dict(nls_parameters) if isinstance(nls_parameters, Mapping) else nls_parameters
        normalized_ctx = (
            {ns: dict(attrs) for ns, attrs in resolved_contexts.items()}
            if isinstance(resolved_contexts, Mapping)
            else resolved_contexts
        )
        self.contexts = normalized_ctx
        self.context_attributes = normalized_ctx
        self.ebs_apps_initialize = ebs_apps_initialize
        self.mo_global_policy = mo_global_policy
        self.apex_session = apex_session
        self.reset_package = bool(reset_package)
        self.clear_identifier = bool(clear_identifier)
        self.clear_context_namespaces: tuple[str, ...] = tuple(clear_context_namespaces)
        self.apex_detach = bool(apex_detach)
        self.reset_on_release = bool(reset_on_release)

    def __repr__(self) -> str:
        return (
            f"OracleSessionContextConfig(module={self.module!r}, action={self.action!r}, "
            f"client_identifier={self.client_identifier!r}, ecid={self.ecid!r}, "
            f"edition={self.edition!r}, time_zone={self.time_zone!r}, "
            f"nls_parameters={self.nls_parameters!r}, contexts={self.contexts!r}, "
            f"reset_on_release={self.reset_on_release!r})"
        )

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, OracleSessionContextConfig):
            return NotImplemented
        return (
            self.clientinfo,
            self.current_schema,
            self.dbop,
            self.end_user_security,
            self.transaction_priority,
            self.module,
            self.action,
            self.client_identifier,
            self.ecid,
            self.edition,
            self.time_zone,
            self.nls_parameters,
            self.contexts,
            self.ebs_apps_initialize,
            self.mo_global_policy,
            self.apex_session,
            self.reset_package,
            self.clear_identifier,
            self.clear_context_namespaces,
            self.apex_detach,
            self.reset_on_release,
        ) == (
            other.clientinfo,
            other.current_schema,
            other.dbop,
            other.end_user_security,
            other.transaction_priority,
            other.module,
            other.action,
            other.client_identifier,
            other.ecid,
            other.edition,
            other.time_zone,
            other.nls_parameters,
            other.contexts,
            other.ebs_apps_initialize,
            other.mo_global_policy,
            other.apex_session,
            other.reset_package,
            other.clear_identifier,
            other.clear_context_namespaces,
            other.apex_detach,
            other.reset_on_release,
        )

    def __hash__(self) -> int:
        return hash((
            self.clientinfo,
            self.current_schema,
            self.dbop,
            self.end_user_security,
            self.transaction_priority,
            self.module,
            self.action,
            self.client_identifier,
            self.ecid,
            self.edition,
            self.time_zone,
            self.ebs_apps_initialize,
            self.mo_global_policy,
            self.apex_session,
            self.reset_package,
            self.clear_identifier,
            self.clear_context_namespaces,
            self.apex_detach,
            self.reset_on_release,
        ))


def connection_is_thin(connection: object) -> bool:
    """Return whether an Oracle connection is in Thin mode."""
    thin = getattr(connection, "thin", None)
    if thin is None:
        return True
    return bool(thin)


def supports_direct_path_load(connection: object) -> bool:
    """Return whether a connection supports direct path load.

    The declared ``oracledb>=3.4`` floor guarantees the API on a real
    ``oracledb`` connection, but a proxy or test double may not carry it, and
    the documented behaviour is to fall back rather than raise.

    Args:
        connection: The connection about to be used for ingestion.

    Returns:
        True when the connection is Thin-mode and exposes the API.
    """
    return connection_is_thin(connection) and hasattr(connection, "direct_path_load")


def supports_df_batches(connection: object) -> bool:
    """Return whether a connection exposes DataFrame batch fetches.

    ``fetch_df_batches`` landed in python-oracledb 3.0, below the declared
    floor, but a proxy or test double may still not carry it.

    Args:
        connection: The connection about to be used for Arrow batch fetches.

    Returns:
        True when the connection exposes the API.
    """
    return hasattr(connection, "fetch_df_batches")


_FETCH_KWARGS_CACHE: dict[tuple[object, object], dict[str, object]] = {}


def build_fetch_kwargs(driver_features: "dict[str, Any]") -> "dict[str, object]":
    """Build per-statement Oracle fetch keyword arguments."""
    fetch_lobs = driver_features.get("fetch_lobs")
    fetch_decimals = driver_features.get("fetch_decimals")
    cache_key = (fetch_lobs, fetch_decimals)
    cached = _FETCH_KWARGS_CACHE.get(cache_key)
    if cached is not None:
        return cached

    fetch_kwargs: dict[str, object] = {}
    if fetch_lobs is not None:
        fetch_kwargs["fetch_lobs"] = fetch_lobs
    if fetch_decimals is not None:
        fetch_kwargs["fetch_decimals"] = fetch_decimals
    _FETCH_KWARGS_CACHE[cache_key] = fetch_kwargs
    return fetch_kwargs


def build_arrow_fetch_kwargs(
    driver_features_or_batch_size: "Mapping[str, Any] | int | None" = None,
    driver_features: "Mapping[str, Any] | None" = None,
    arrow_schema: Any = None,
    *,
    batch_size: int | None = None,
) -> "dict[str, object]":
    """Build Oracle Arrow/DataFrame fetch keyword arguments.

    ``oracledb.Connection.fetch_df_all`` and ``fetch_df_batches`` accept only
    ``statement``, ``parameters``, ``arraysize``, ``fetch_decimals``, and
    ``requested_schema``; passing ``fetch_lobs`` raises ``TypeError``.
    """
    if isinstance(driver_features_or_batch_size, Mapping):
        resolved_features: Mapping[str, Any] | None = driver_features_or_batch_size
        resolved_batch_size = batch_size
    else:
        resolved_features = driver_features
        resolved_batch_size = batch_size if batch_size is not None else driver_features_or_batch_size

    fetch_kwargs: dict[str, object] = {}
    if resolved_batch_size is not None:
        fetch_kwargs["arraysize"] = resolved_batch_size
    if resolved_features is not None:
        fetch_decimals = resolved_features.get("fetch_decimals")
        if fetch_decimals is not None:
            fetch_kwargs["fetch_decimals"] = fetch_decimals
        requested_schema = resolved_features.get("arrow_requested_schema")
        if arrow_schema is None and requested_schema is not None:
            fetch_kwargs["requested_schema"] = requested_schema
    if arrow_schema is not None:
        fetch_kwargs["requested_schema"] = arrow_schema
    return fetch_kwargs


def normalize_column_names(column_names: "list[str]", driver_features: "dict[str, Any]") -> "list[str]":
    should_lowercase = driver_features.get("enable_lowercase_column_names", False)
    if not should_lowercase:
        return column_names
    normalized: list[str] = []
    for name in column_names:
        if name and IMPLICIT_UPPER_COLUMN_PATTERN.fullmatch(name):
            normalized.append(name.lower())
        else:
            normalized.append(name)
    return normalized


def normalize_execute_many_parameters_sync(parameters: Any) -> Any:
    """Normalize parameters for Oracle executemany calls.

    Args:
        parameters: Prepared parameters payload.

    Returns:
        Normalized parameters payload.
    """
    if isinstance(parameters, tuple):
        return list(parameters)
    return parameters


def normalize_execute_many_parameters_async(parameters: Any) -> Any:
    """Normalize parameters for Oracle async executemany calls.

    Args:
        parameters: Prepared parameters payload.

    Returns:
        Normalized parameters payload.
    """
    return normalize_execute_many_parameters_sync(parameters)


def coerce_large_parameters_sync(
    connection: Any,
    parameters: Any,
    *,
    clob_type: Any = DB_TYPE_CLOB,
    blob_type: Any = DB_TYPE_BLOB,
    varchar2_byte_limit: int = 4000,
    raw_byte_limit: int = 2000,
    version_cache: Any = None,
) -> Any:
    """Coerce large string/bytes parameters into CLOBs/BLOBs with wrapper-aware routing.

    Routing order:
        1. ``OracleClob`` / ``OracleBlob`` / ``OracleJson`` wrappers — explicit user
            intent, length-thresholds bypassed.
        2. Plain ``str`` whose UTF-8 encoding exceeds ``varchar2_byte_limit`` → CLOB.
        3. Plain ``bytes``/``bytearray`` longer than ``raw_byte_limit`` → BLOB.

    Handles ``dict`` (named binds), ``tuple`` and ``list`` (positional binds).
    Tuples are returned as new lists when any value is rewritten so the driver's
    ``cast(... | tuple | list | dict)`` contract is preserved.

    Args:
        connection: Oracle database connection.
        parameters: Prepared parameters payload.
        clob_type: Oracle CLOB DB type (``oracledb.DB_TYPE_CLOB``).
        blob_type: Oracle BLOB DB type (``oracledb.DB_TYPE_BLOB``).
        varchar2_byte_limit: Byte-length threshold for implicit CLOB conversion.
        raw_byte_limit: Byte-length threshold for implicit BLOB conversion.
        version_cache: Optional pool-scoped Oracle version cache.

    Returns:
        Parameters payload with values routed to the correct LOB / native type.
    """
    if not parameters:
        return parameters
    return _coerce_parameters_sync(
        connection,
        parameters,
        clob_type=clob_type,
        blob_type=blob_type,
        varchar2_byte_limit=varchar2_byte_limit,
        raw_byte_limit=raw_byte_limit,
        json_binding_state=_OracleJsonBindingState(connection, version_cache),
    )


async def coerce_large_parameters_async(
    connection: Any,
    parameters: Any,
    *,
    clob_type: Any = DB_TYPE_CLOB,
    blob_type: Any = DB_TYPE_BLOB,
    varchar2_byte_limit: int = 4000,
    raw_byte_limit: int = 2000,
    version_cache: Any = None,
) -> Any:
    """Async mirror of :func:`coerce_large_parameters_sync`.

    Identical routing order; ``connection.createlob`` is awaited for the async
    Oracle driver.
    """
    if not parameters:
        return parameters
    return await _coerce_parameters_async(
        connection,
        parameters,
        clob_type=clob_type,
        blob_type=blob_type,
        varchar2_byte_limit=varchar2_byte_limit,
        raw_byte_limit=raw_byte_limit,
        json_binding_state=_OracleJsonBindingState(connection, version_cache),
    )


def coerce_many_parameters_sync(
    connection: Any,
    parameters: Any,
    *,
    clob_type: Any = DB_TYPE_CLOB,
    blob_type: Any = DB_TYPE_BLOB,
    varchar2_byte_limit: int = 4000,
    raw_byte_limit: int = 2000,
    version_cache: Any = None,
) -> Any:
    """Coerce every parameter row prepared for synchronous ``executemany``."""
    normalized = normalize_execute_many_parameters_sync(parameters)
    if not normalized:
        return normalized
    json_binding_state = _OracleJsonBindingState(connection, version_cache)
    coerced_rows: list[Any] | None = None
    for index, row in enumerate(normalized):
        coerced_row = _coerce_parameters_sync(
            connection,
            row,
            clob_type=clob_type,
            blob_type=blob_type,
            varchar2_byte_limit=varchar2_byte_limit,
            raw_byte_limit=raw_byte_limit,
            json_binding_state=json_binding_state,
        )
        if coerced_rows is None:
            if coerced_row is row:
                continue
            coerced_rows = list(normalized[:index])
        coerced_rows.append(coerced_row)
    return normalized if coerced_rows is None else coerced_rows


async def coerce_many_parameters_async(
    connection: Any,
    parameters: Any,
    *,
    clob_type: Any = DB_TYPE_CLOB,
    blob_type: Any = DB_TYPE_BLOB,
    varchar2_byte_limit: int = 4000,
    raw_byte_limit: int = 2000,
    version_cache: Any = None,
) -> Any:
    """Coerce every parameter row prepared for asynchronous ``executemany``."""
    normalized = normalize_execute_many_parameters_async(parameters)
    if not normalized:
        return normalized
    json_binding_state = _OracleJsonBindingState(connection, version_cache)
    coerced_rows: list[Any] | None = None
    for index, row in enumerate(normalized):
        coerced_row = await _coerce_parameters_async(
            connection,
            row,
            clob_type=clob_type,
            blob_type=blob_type,
            varchar2_byte_limit=varchar2_byte_limit,
            raw_byte_limit=raw_byte_limit,
            json_binding_state=json_binding_state,
        )
        if coerced_rows is None:
            if coerced_row is row:
                continue
            coerced_rows = list(normalized[:index])
        coerced_rows.append(coerced_row)
    return normalized if coerced_rows is None else coerced_rows


def build_insert_statement(table: str, columns: "list[str]") -> str:
    column_list = ", ".join(_format_insert_column(col) for col in columns)
    placeholders = ", ".join(f":{idx + 1}" for idx in range(len(columns)))
    return f"INSERT INTO {table} ({column_list}) VALUES ({placeholders})"


def build_truncate_statement(table: str) -> str:
    return f"TRUNCATE TABLE {table}"


def build_pipeline_stack_result(
    statement: "SQL",
    method: str,
    returns_rows: bool,
    parameters: Any,
    pipeline_result: Any,
    driver_features: "Mapping[str, Any]",
    *,
    coerced_rows: Sequence[Any] | None = None,
) -> "StackResult":
    """Build StackResult from Oracle pipeline output.

    Args:
        statement: Statement executed in the pipeline.
        method: Pipeline execution method name.
        returns_rows: Whether the operation returns rows.
        parameters: Prepared parameters used for execution.
        pipeline_result: Raw pipeline execution result.
        driver_features: Driver feature configuration for normalization.
        coerced_rows: Optional LOB-materialized pipeline rows.

    Returns:
        StackResult for the pipeline operation.
    """
    try:
        rows = pipeline_result.rows
    except AttributeError:
        rows = None
    try:
        columns = pipeline_result.columns
    except AttributeError:
        columns = None

    if coerced_rows is not None:
        rows = coerced_rows
    data: list[dict[str, Any]] | None = None
    if returns_rows:
        if not rows:
            data = []
        else:
            if columns:
                names = []
                for index, column in enumerate(columns):
                    try:
                        name = column.name
                    except AttributeError:
                        name = f"column_{index}"
                    names.append(name)
            else:
                first = rows[0]
                names = [f"column_{index}" for index in range(len(first) if isinstance(first, Sized) else 0)]
            names = normalize_column_names(names, dict(driver_features))
            normalized_rows: list[dict[str, Any]] = []
            for row in rows:
                if isinstance(row, dict):
                    normalized_rows.append(row)
                else:
                    normalized_rows.append(dict(zip(names, row, strict=False)))
            data = normalized_rows

    metadata: dict[str, Any] = {"pipeline_operation": method}
    try:
        warning = pipeline_result.warning
    except AttributeError:
        warning = None
    if warning is not None:
        metadata["warning"] = warning

    try:
        return_value = pipeline_result.return_value
    except AttributeError:
        return_value = None
    if return_value is not None:
        metadata["return_value"] = return_value

    if method == "execute_many":
        try:
            rows_affected = len(parameters or ())
        except TypeError:
            rows_affected = 0
    elif method == "execute" and not returns_rows:
        rows_affected = 1
    elif returns_rows:
        rows_affected = len(data or [])
    else:
        rows_affected = 0

    sql_result = create_sql_result(statement, data=data, rows_affected=rows_affected, metadata=metadata)
    return StackResult.from_sql_result(sql_result)


def resolve_rowcount(cursor: Any) -> int:
    """Resolve rowcount from an Oracle cursor.

    Args:
        cursor: Oracle cursor with optional rowcount metadata.

    Returns:
        Rowcount value or 0 when unavailable.
    """
    if not has_rowcount(cursor):
        return 0
    rowcount = cursor.rowcount
    if isinstance(rowcount, int) and rowcount >= 0:
        return rowcount
    return 0


ORACLE_CLIENT_INFO_MAX_BYTES: int = 64
ORACLE_DBOP_MAX_BYTES: int = 64
ORACLE_MODULE_MAX_BYTES: int = 48
ORACLE_ACTION_MAX_BYTES: int = 64
ORACLE_CLIENT_IDENTIFIER_MAX_BYTES: int = 64
ORACLE_ECID_MAX_BYTES: int = 64


def truncate_utf8_bytes(value: str, max_bytes: int) -> str:
    """Truncate a string to at most max_bytes in UTF-8 without splitting multi-byte codepoints.

    Args:
        value: Input string to truncate.
        max_bytes: Maximum UTF-8 byte length allowed.

    Returns:
        UTF-8 safe truncated string.
    """
    encoded = value.encode("utf-8")
    if len(encoded) <= max_bytes:
        return value
    return encoded[:max_bytes].decode("utf-8", errors="ignore")


ORACLE_SESSIONLESS_TXN_ID_MAX_BYTES: Final[int] = 64


def supports_opentelemetry_tracing(connection: Any) -> bool:
    """Probe native trace support without raising on closed or older connections."""
    try:
        return bool(getattr(connection, "supports_opentelemetry_tracing", False))
    except Exception:
        return False


def clear_oracle_otel_trace_context(connection: Any) -> None:
    """Clear native trace state, propagating reset failures to the lease owner."""
    method = getattr(connection, "set_opentelemetry_trace_context", None)
    if method is None:
        return
    if bool(getattr(connection, "supports_opentelemetry_tracing", False)):
        method(None)


def validate_sessionless_transaction_args(
    transaction_id: str | bytes | None, *, timeout: int, is_resume: bool = False, defer_round_trip: bool = True
) -> bytes | None:
    """Validate an optional global transaction identifier before native operations."""
    if (
        not isinstance(defer_round_trip, bool)
        or isinstance(timeout, bool)
        or not isinstance(timeout, int)
        or timeout < (0 if is_resume else 1)
    ):
        msg = "Sessionless timeout must be positive (nonnegative on resume) and defer_round_trip must be bool"
        raise ImproperConfigurationError(msg)
    if transaction_id is None and not is_resume:
        return None
    if not isinstance(transaction_id, (str, bytes)):
        msg = "Sessionless transaction_id must be a string or bytes"
        raise ImproperConfigurationError(msg)
    resolved = transaction_id.encode("utf-8") if isinstance(transaction_id, str) else transaction_id
    if not 1 <= len(resolved) <= ORACLE_SESSIONLESS_TXN_ID_MAX_BYTES:
        msg = "Sessionless transaction_id must contain 1..64 UTF-8 bytes"
        raise ImproperConfigurationError(msg)
    return resolved


def validate_sessionless_execution_options(*, suspend_on_success: bool = False, batch_size: int | None = None) -> None:
    """Refuse invalid suspension and batch combinations before issuing SQL."""
    if (
        not isinstance(suspend_on_success, bool)
        or (
            batch_size is not None
            and (isinstance(batch_size, bool) or not isinstance(batch_size, int) or batch_size <= 0)
        )
        or (suspend_on_success and batch_size is not None)
    ):
        msg = "suspend_on_success must be bool and cannot be combined with a positive batch_size"
        raise ImproperConfigurationError(msg)


async def coerce_pipeline_rows_async(
    rows: Sequence[Any] | None, driver_features: Mapping[str, Any]
) -> list[Any] | None:
    """Materialize LOB cells while preserving scalar pipeline rows on a fast path."""
    if rows is None:
        return None
    if driver_features.get("fetch_lobs", False):
        return list(rows)
    result: list[Any] = []
    for row in rows:
        cells = row.values() if isinstance(row, Mapping) else row
        if not any(is_async_readable(cell) or is_readable(cell) for cell in cells):
            result.append(row)
            continue
        keys = list(row) if isinstance(row, Mapping) else None
        values = row.values() if isinstance(row, Mapping) else row
        converted: list[Any] = []
        for cell in values:
            if is_async_readable(cell):
                cell = await cell.read()
            elif is_readable(cell):
                cell = cell.read()
            converted.append(cell)
        result.append(dict(zip(keys, converted, strict=True)) if keys is not None else tuple(converted))
    return result


def apply_oracle_session_trace_attributes(
    connection: Any,
    *,
    module: "str | None" = None,
    action: "str | None" = None,
    client_identifier: "str | None" = None,
    econtext_id: "str | None" = None,
    ecid: "str | None" = None,
    preserve_client_identifier: bool = False,
    enable_session_attributes: bool = True,
    enable_otel_trace_context: bool = False,
) -> None:
    """Set Oracle end-to-end tracing attributes on connection for zero-RTT OALL8 piggybacking.

    Args:
        connection: Oracle sync or async connection object.
        module: Optional explicit V$SESSION.MODULE value (truncated to 48 UTF-8 bytes).
        action: Optional explicit V$SESSION.ACTION value (truncated to 64 UTF-8 bytes).
        client_identifier: Optional explicit V$SESSION.CLIENT_IDENTIFIER value (truncated to 64 UTF-8 bytes).
        econtext_id: Optional explicit V$SESSION.ECID value (truncated to 64 UTF-8 bytes).
        ecid: Optional alias for ``econtext_id``.
        preserve_client_identifier: Keep an active tenant identifier unchanged.
        enable_session_attributes: Apply traditional Oracle diagnostic attributes.
        enable_otel_trace_context: Propagate W3C headers through supported native APIs.
    """
    ctx = SQLCommenterContext.get() or {}
    resolved_module = (
        module if module is not None else (ctx.get("module") or ctx.get("framework") or ctx.get("controller"))
    )
    resolved_action = action if action is not None else (ctx.get("action") or ctx.get("route"))
    resolved_client_id = (
        client_identifier
        if client_identifier is not None
        else (CorrelationContext.get() or ctx.get("correlation_id") or ctx.get("client_identifier"))
    )
    resolved_ecid = econtext_id if econtext_id is not None else ecid
    if resolved_ecid is None:
        trace_id, span_id = get_trace_context()
        if trace_id and span_id:
            resolved_ecid = f"00-{trace_id}-{span_id}-01"
        elif trace_id:
            resolved_ecid = trace_id
        else:
            resolved_ecid = ctx.get("traceparent") or ctx.get("econtext_id") or ctx.get("ecid")

    if enable_session_attributes and resolved_module is not None:
        with contextlib.suppress(Exception):
            connection.module = truncate_utf8_bytes(resolved_module, ORACLE_MODULE_MAX_BYTES)
    if enable_session_attributes and resolved_action is not None:
        with contextlib.suppress(Exception):
            connection.action = truncate_utf8_bytes(resolved_action, ORACLE_ACTION_MAX_BYTES)
    if enable_session_attributes and resolved_client_id is not None and not preserve_client_identifier:
        with contextlib.suppress(Exception):
            connection.client_identifier = truncate_utf8_bytes(resolved_client_id, ORACLE_CLIENT_IDENTIFIER_MAX_BYTES)
    if enable_session_attributes and resolved_ecid is not None and not connection_is_thin(connection):
        truncated_ecid = truncate_utf8_bytes(resolved_ecid, ORACLE_ECID_MAX_BYTES)
        with contextlib.suppress(Exception):
            try:
                connection.econtext_id = truncated_ecid
            except AttributeError:
                connection.ecid = truncated_ecid
    if enable_otel_trace_context and supports_opentelemetry_tracing(connection):
        try:
            traceparent, tracestate = get_w3c_trace_context()
            if traceparent is None:
                connection.set_opentelemetry_trace_context(None)
            else:
                connection.set_opentelemetry_trace_context(traceparent, tracestate)
        except Exception as exc:
            import logging

            logging.getLogger(__name__).debug("Oracle trace context propagation failed: %s", exc)


def apply_driver_features(
    statement_config: "StatementConfig", driver_features: "Mapping[str, Any] | None"
) -> "tuple[StatementConfig, dict[str, Any]]":
    """Apply OracleDB driver feature defaults and validate known keys.

    Defaults ``fetch_lobs`` to ``False`` so direct LOB reads under the 1 GB
    fetch ceiling return text/bytes by default. Locators remain available for
    OUT binds, PL/SQL, and explicit ``fetch_lobs=True``.
    """
    if driver_features:
        unknown_keys = sorted(set(driver_features.keys()) - VALID_ORACLE_DRIVER_FEATURE_KEYS)
        if unknown_keys:
            msg = f"Unknown Oracle driver_features key(s): {', '.join(unknown_keys)}"
            raise ImproperConfigurationError(msg)
    features: dict[str, Any] = dict(driver_features) if driver_features else {}
    features.setdefault("enable_otel_trace_context", True)
    features.setdefault("enable_auto_state_tagging", True)
    features.setdefault("enable_numpy_vectors", NUMPY_INSTALLED)
    features.setdefault("enable_lowercase_column_names", True)
    features.setdefault("enable_uuid_binary", True)
    if "enable_lob_fetch" in features and "fetch_lobs" not in features:
        features["fetch_lobs"] = bool(features["enable_lob_fetch"])
    features.setdefault("fetch_lobs", False)
    if "vector_return_format" not in features:
        if not NUMPY_INSTALLED:
            features["vector_return_format"] = "list"
        elif features["enable_numpy_vectors"]:
            features["vector_return_format"] = "numpy"
        else:
            features["vector_return_format"] = "array"
    features.setdefault("oracle_varchar2_byte_limit", 4000)
    features.setdefault("oracle_raw_byte_limit", 2000)
    session_hook = features.get("session_context_hook")
    if isinstance(session_hook, Mapping):
        features["session_context_hook"] = OracleSessionContextConfig(**session_hook)
    features.setdefault("enable_session_tracing", False)
    return statement_config, features


def _extract_column_cache_key(description: "list[Any]") -> "tuple[tuple[object, object], ...]":
    key_items: list[tuple[object, object]] = []
    for column in description:
        try:
            col_name = column[0]
            type_code = column[1]
        except (TypeError, IndexError, KeyError):
            col_name = getattr(column, "name", None)
            type_code = getattr(column, "type_code", None)
        type_identifier = getattr(type_code, "name", None) or type_code
        key_items.append((col_name, type_identifier))
    return tuple(key_items)


def resolve_row_metadata(
    description: "list[Any] | None", driver_features: "dict[str, Any]", cache: "dict[Any, tuple[Any, list[str], bool]]"
) -> "tuple[list[str], bool]":
    """Resolve and cache Oracle row metadata for hot row materialization paths.

    Args:
        description: Cursor description metadata.
        driver_features: Driver feature configuration.
        cache: Driver-local metadata cache.

    Returns:
        Tuple of (normalized column names, requires_lob_coercion).
    """
    if not description:
        return [], False

    cache_key = _extract_column_cache_key(description)
    cached = cache.get(cache_key)
    if cached is not None:
        return cached[1], cached[2]

    column_names: list[str] = []
    requires_lob_coercion = False
    for column in description:
        try:
            column_names.append(column[0])
            type_code = column[1]
        except (TypeError, IndexError, KeyError):
            column_name = getattr(column, "name", None)
            if isinstance(column_name, str):
                column_names.append(column_name)
            type_code = getattr(column, "type_code", None)
            if type_code is None:
                requires_lob_coercion = True
                continue

        if requires_lob_coercion:
            continue

        type_name = getattr(type_code, "name", None)
        if isinstance(type_name, str):
            upper_name = type_name.upper()
            if any(marker in upper_name for marker in _LOB_TYPE_NAME_MARKERS):
                requires_lob_coercion = True
            continue

        type_text = str(type_code).upper()
        if any(marker in type_text for marker in _LOB_TYPE_NAME_MARKERS):
            requires_lob_coercion = True

    normalized_column_names = normalize_column_names(column_names, driver_features)

    if len(cache) >= ROW_CACHE_MAX_SIZE:
        cache.pop(next(iter(cache)))
    cache[cache_key] = (description, normalized_column_names, requires_lob_coercion)
    return normalized_column_names, requires_lob_coercion


class OracleSyncStreamSource:
    """Compiled chunk source streaming dict rows from an oracledb cursor via ``fetchmany``."""

    __slots__ = (
        "_chunk_size",
        "_column_names",
        "_cursor",
        "_driver",
        "_fetch_lobs",
        "_parameters",
        "_requires_lob_coercion",
        "_sql",
    )

    def __init__(
        self,
        driver: "_OracleSyncStreamDriver",
        sql: str,
        parameters: "list[object] | tuple[object, ...] | dict[object, object] | None",
        chunk_size: int,
        fetch_lobs: bool | None = None,
    ) -> None:
        self._driver = driver
        self._sql = sql
        self._parameters = parameters
        self._chunk_size = chunk_size
        self._fetch_lobs = fetch_lobs
        self._cursor: OracleSyncRawCursor | None = None
        self._column_names: list[str] | None = None
        self._requires_lob_coercion: bool | None = None

    def start(self) -> None:
        handler = self._driver.handle_database_exceptions()
        self._driver._run_with_exception_handler(handler, self._start)
        self._driver._check_pending_exception(handler)

    def _start(self) -> None:
        self._driver._apply_session_tracing(self._driver.connection)
        try:
            cursor = self._driver.connection.cursor()
            self._cursor = cursor
            cursor.arraysize = self._chunk_size
            cursor.prefetchrows = self._chunk_size
            fetch_kwargs = getattr(self._driver, "_fetch_kwargs", None) or build_fetch_kwargs(
                self._driver.driver_features
            )
            if self._fetch_lobs is not None:
                fetch_kwargs = {**fetch_kwargs, "fetch_lobs": self._fetch_lobs}
            parameters = coerce_large_parameters_sync(
                self._driver.connection,
                self._parameters,
                clob_type=DB_TYPE_CLOB,
                blob_type=DB_TYPE_BLOB,
                varchar2_byte_limit=self._driver.driver_features.get("oracle_varchar2_byte_limit", 4000),
                raw_byte_limit=self._driver.driver_features.get("oracle_raw_byte_limit", 2000),
                version_cache=getattr(self._driver, "_oracle_version_cache", None),
            )
            cast("Any", cursor).execute(self._sql, parameters or {}, **fetch_kwargs)
        except BaseException:
            self.close(error=True)
            raise

    def fetch_chunk(self) -> "list[dict[str, Any]]":
        handler = self._driver.handle_database_exceptions()
        cursor = self._cursor
        if cursor is None:
            return []
        fetched = self._driver._run_with_exception_handler(handler, cursor.fetchmany, self._chunk_size)
        self._driver._check_pending_exception(handler)
        if not fetched:
            return []
        rows: list[Any] = list(fetched)
        column_names = self._column_names
        if column_names is None:
            column_names, requires_lob_coercion = self._driver._resolve_row_metadata(cursor.description)
            self._column_names = column_names
            self._requires_lob_coercion = requires_lob_coercion
        if self._caller_wants_locators():
            return rows_to_dicts(rows, column_names)
        coerced_rows, column_names = collect_sync_rows(
            rows,
            cursor.description,
            self._driver.driver_features,
            column_names=column_names,
            requires_lob_coercion=self._requires_lob_coercion,
        )
        return rows_to_dicts(cast("list[Any]", coerced_rows), column_names)

    def close(self, error: bool = False) -> None:
        cursor = self._cursor
        self._cursor = None
        if cursor is not None:
            with contextlib.suppress(Exception):
                cursor.close()

    def _caller_wants_locators(self) -> bool:
        """Report whether the caller asked to receive LOB locators.

        ``fetch_lobs=True`` is a documented escape hatch for reading a LOB
        incrementally, so coercing under it would remove the only reason to ask
        for it.
        """
        fetch_lobs = self._fetch_lobs
        if fetch_lobs is None:
            fetch_lobs = self._driver.driver_features.get("fetch_lobs")
        return bool(fetch_lobs)


class OracleAsyncStreamSource:
    """Compiled async chunk source streaming dict rows from an oracledb cursor via ``fetchmany``."""

    __slots__ = (
        "_chunk_size",
        "_column_names",
        "_cursor",
        "_driver",
        "_fetch_lobs",
        "_parameters",
        "_requires_lob_coercion",
        "_sql",
    )

    def __init__(
        self,
        driver: "_OracleAsyncStreamDriver",
        sql: str,
        parameters: "list[object] | tuple[object, ...] | dict[object, object] | None",
        chunk_size: int,
        fetch_lobs: bool | None = None,
    ) -> None:
        self._driver = driver
        self._sql = sql
        self._parameters = parameters
        self._chunk_size = chunk_size
        self._fetch_lobs = fetch_lobs
        self._cursor: OracleAsyncRawCursor | None = None
        self._column_names: list[str] | None = None
        self._requires_lob_coercion: bool | None = None

    async def start(self) -> None:
        handler = self._driver.handle_database_exceptions()
        await self._driver._run_with_exception_handler(handler, self._start)
        self._driver._check_pending_exception(handler)

    async def fetch_chunk(self) -> "list[dict[str, Any]]":
        handler = self._driver.handle_database_exceptions()
        cursor = self._cursor
        if cursor is None:
            return []
        rows = await self._driver._run_with_exception_handler(handler, cursor.fetchmany, self._chunk_size)
        self._driver._check_pending_exception(handler)
        if not rows:
            return []
        column_names = self._column_names
        if column_names is None:
            column_names, requires_lob_coercion = self._driver._resolve_row_metadata(cursor.description)
            self._column_names = column_names
            self._requires_lob_coercion = requires_lob_coercion
        if self._caller_wants_locators():
            return rows_to_dicts(rows, column_names)
        coerced_rows, column_names = await collect_async_rows(
            rows,
            cursor.description,
            self._driver.driver_features,
            column_names=column_names,
            requires_lob_coercion=self._requires_lob_coercion,
        )
        return rows_to_dicts(cast("list[Any]", coerced_rows), column_names)

    async def close(self, error: bool = False) -> None:
        cursor = self._cursor
        self._cursor = None
        if cursor is not None:
            with contextlib.suppress(Exception):
                cursor.close()

    async def _start(self) -> None:
        self._driver._apply_session_tracing(self._driver.connection)
        try:
            cursor = self._driver.connection.cursor()
            self._cursor = cursor
            cursor.arraysize = self._chunk_size
            cursor.prefetchrows = self._chunk_size
            fetch_kwargs = getattr(self._driver, "_fetch_kwargs", None) or build_fetch_kwargs(
                self._driver.driver_features
            )
            if self._fetch_lobs is not None:
                fetch_kwargs = {**fetch_kwargs, "fetch_lobs": self._fetch_lobs}
            parameters = await coerce_large_parameters_async(
                self._driver.connection,
                self._parameters,
                clob_type=DB_TYPE_CLOB,
                blob_type=DB_TYPE_BLOB,
                varchar2_byte_limit=self._driver.driver_features.get("oracle_varchar2_byte_limit", 4000),
                raw_byte_limit=self._driver.driver_features.get("oracle_raw_byte_limit", 2000),
                version_cache=getattr(self._driver, "_oracle_version_cache", None),
            )
            await cast("Any", cursor).execute(self._sql, parameters or {}, **fetch_kwargs)
        except BaseException:
            await self.close(error=True)
            raise

    def _caller_wants_locators(self) -> bool:
        """Report whether the caller asked to receive LOB locators.

        ``fetch_lobs=True`` is a documented escape hatch for reading a LOB
        incrementally, so coercing under it would remove the only reason to ask
        for it.
        """
        fetch_lobs = self._fetch_lobs
        if fetch_lobs is None:
            fetch_lobs = self._driver.driver_features.get("fetch_lobs")
        return bool(fetch_lobs)


def collect_sync_rows(
    fetched_data: "list[Any] | None",
    description: "list[Any] | None",
    driver_features: "dict[str, Any]",
    *,
    column_names: "list[str] | None" = None,
    requires_lob_coercion: "bool | None" = None,
) -> "tuple[list[tuple[Any, ...]], list[str]]":
    """Collect OracleDB sync rows as tuples with normalized column names.

    LOB locators are still read for OUT binds, PL/SQL, and explicit
    ``fetch_lobs=True``. Direct-fetched LOB values under the 1 GB ceiling
    arrive as plain strings/bytes and pass through unchanged. The raw coerced
    tuples are returned instead of dicts so that ``SQLResult`` can handle lazy
    dict materialization based on ``row_format``.

    Args:
        fetched_data: Rows returned from cursor.fetchall().
        description: Cursor description metadata.
        driver_features: Driver feature configuration.
        column_names: Optional precomputed normalized column names.
        requires_lob_coercion: Optional precomputed LOB-coercion flag.

    Returns:
        Tuple of (rows, column_names).
    """
    if not description:
        return [], []
    resolved_column_names = (
        normalize_column_names([col[0] for col in description], driver_features)
        if column_names is None
        else column_names
    )
    if not fetched_data:
        return [], resolved_column_names

    if driver_features.get("fetch_lobs"):
        return [row if isinstance(row, tuple) else tuple(row) for row in fetched_data], resolved_column_names

    if requires_lob_coercion is None:
        requires_lob_coercion = _description_requires_lob_coercion(description)
    if not requires_lob_coercion:
        first_row = fetched_data[0]
        first_row_tuple = first_row if isinstance(first_row, tuple) else tuple(first_row)
        if not _row_requires_lob_coercion(first_row_tuple):
            return cast("list[tuple[Any, ...]]", fetched_data), resolved_column_names

    data: list[tuple[Any, ...]] = []
    for row in fetched_data:
        row_tuple = row if isinstance(row, tuple) else tuple(row)
        data.append(_coerce_sync_row_values(row_tuple))
    return data, resolved_column_names


async def collect_async_rows(
    fetched_data: "list[Any] | None",
    description: "list[Any] | None",
    driver_features: "dict[str, Any]",
    *,
    column_names: "list[str] | None" = None,
    requires_lob_coercion: "bool | None" = None,
) -> "tuple[list[tuple[Any, ...]], list[str]]":
    """Collect OracleDB async rows as tuples with normalized column names.

    LOB locators are still read for OUT binds, PL/SQL, and explicit
    ``fetch_lobs=True``. Direct-fetched LOB values under the 1 GB ceiling
    arrive as plain strings/bytes and pass through unchanged. The raw coerced
    tuples are returned instead of dicts so that ``SQLResult`` can handle lazy
    dict materialization based on ``row_format``.

    Args:
        fetched_data: Rows returned from cursor.fetchall().
        description: Cursor description metadata.
        driver_features: Driver feature configuration.
        column_names: Optional precomputed normalized column names.
        requires_lob_coercion: Optional precomputed LOB-coercion flag.

    Returns:
        Tuple of (rows, column_names).
    """
    if not description:
        return [], []
    resolved_column_names = (
        normalize_column_names([col[0] for col in description], driver_features)
        if column_names is None
        else column_names
    )
    if not fetched_data:
        return [], resolved_column_names

    if driver_features.get("fetch_lobs"):
        return [row if isinstance(row, tuple) else tuple(row) for row in fetched_data], resolved_column_names

    if requires_lob_coercion is None:
        requires_lob_coercion = _description_requires_lob_coercion(description)
    if not requires_lob_coercion:
        first_row = fetched_data[0]
        first_row_tuple = first_row if isinstance(first_row, tuple) else tuple(first_row)
        if not _row_requires_lob_coercion(first_row_tuple):
            return cast("list[tuple[Any, ...]]", fetched_data), resolved_column_names

    data: list[tuple[Any, ...]] = []
    for row in fetched_data:
        row_tuple = row if isinstance(row, tuple) else tuple(row)
        data.append(await _coerce_async_row_values(row_tuple))
    return data, resolved_column_names


def create_mapped_exception(error: Any, *, logger: Any | None = None) -> SQLSpecError:
    """Map Oracle exceptions to SQLSpec exceptions.

    This is a factory function that returns an exception instance rather than
    raising. This pattern is more robust for use in __exit__ handlers and
    avoids issues with exception control flow in different Python versions.

    Args:
        error: The Oracle exception to map
        logger: Optional logger accepted for adapter signature parity.

    Returns:
        A SQLSpec exception that wraps the original error
    """
    del logger
    if not isinstance(error, BaseException):
        error = oracledb_module.DatabaseError(error)
    error_obj = error.args[0] if getattr(error, "args", None) else None
    if not error_obj:
        return _create_oracle_error(error, None, SQLSpecError, "database error")

    full_code = getattr(error_obj, "full_code", None)
    if isinstance(full_code, str):
        dpy_mapping = _DPY_ERROR_CODE_MAPPING.get(full_code)
        if dpy_mapping:
            error_class, error_desc = dpy_mapping
            return _create_oracle_error(error, None, error_class, f"{error_desc} [{full_code}]")

    try:
        error_code = error_obj.code
    except AttributeError:
        error_code = None

    if isinstance(error_code, str):
        dpy_mapping = _DPY_ERROR_CODE_MAPPING.get(error_code)
        if dpy_mapping:
            error_class, error_desc = dpy_mapping
            return _create_oracle_error(error, None, error_class, f"{error_desc} [{error_code}]")

    if not isinstance(error_code, int) or isinstance(error_code, bool) or not error_code:
        error_text = str(error_obj)
        for dpy_code, (error_class, error_desc) in _DPY_ERROR_CODE_MAPPING.items():
            if dpy_code in error_text:
                return _create_oracle_error(error, None, error_class, f"{error_desc} [{dpy_code}]")
        ora_match = re.search(r"ORA-(\d+)", error_text)
        if ora_match:
            error_code = int(ora_match.group(1))
        else:
            return _create_oracle_error(error, None, SQLSpecError, "database error")

    mapping = _ERROR_CODE_MAPPING.get(error_code)
    if mapping:
        error_class, error_desc = mapping
        return _create_oracle_error(error, error_code, error_class, error_desc)

    if ORA_INTEGRITY_RANGE_START <= error_code < ORA_INTEGRITY_RANGE_END:
        return _create_oracle_error(error, error_code, IntegrityError, "integrity constraint violation")

    if ORA_PARSING_RANGE_START <= error_code < ORA_PARSING_RANGE_END:
        return _create_oracle_error(error, error_code, SQLParsingError, "SQL syntax error")

    return _create_oracle_error(error, error_code, SQLSpecError, "database error")


def build_profile() -> "DriverParameterProfile":
    """Create the OracleDB driver parameter profile."""
    return DriverParameterProfile(
        name="OracleDB",
        default_style=ParameterStyle.NAMED_COLON,
        supported_styles={ParameterStyle.NAMED_COLON, ParameterStyle.NUMERIC, ParameterStyle.QMARK},
        default_execution_style=ParameterStyle.NAMED_COLON,
        supported_execution_styles={ParameterStyle.NAMED_COLON, ParameterStyle.POSITIONAL_COLON},
        has_native_list_expansion=False,
        preserve_parameter_format=True,
        needs_static_script_compilation=False,
        allow_mixed_parameter_styles=False,
        preserve_original_params_for_many=False,
        json_serializer_strategy="driver",
        custom_type_coercions={**build_uuid_coercions(native=True)},
        default_dialect="oracle",
    )


def build_statement_config(*, json_serializer: "Callable[[Any], str] | None" = None) -> StatementConfig:
    """Construct the OracleDB statement configuration with optional JSON serializer."""
    serializer = json_serializer or to_json
    profile = driver_profile
    return build_statement_config_from_profile(
        profile, statement_overrides={"dialect": "oracle"}, json_serializer=serializer
    )


driver_profile = build_profile()

default_statement_config = build_statement_config()


def extract_oracle_wallet_zip(
    wallet_zip: "Path | str | bytes",
    target_dir: "Path | None" = None,
    *,
    prefix: str = "sqlspec_oracle_wallet_",
    wallet_password: "str | bytes | None" = None,
) -> Path:
    """Extract or prepare an Oracle mTLS wallet into a 0o700 directory.

    Supports ``.zip`` wallet archives (as file paths or raw ``bytes``) and
    pre-extracted wallet directories. Validates archive entries against path
    traversal (ZipSlip), enforces ``0o600`` permissions on wallet files,
    converts PKCS#12 ``ewallet.p12`` bundles to ``ewallet.pem`` when
    ``ewallet.pem`` is absent, and reuses a deterministic SHA-256
    content-hashed temporary directory when ``target_dir`` is omitted.

    Args:
        wallet_zip: Path to a wallet ``.zip`` archive, raw archive bytes, or
            path to an existing extracted wallet directory.
        target_dir: Optional directory to extract into. When ``None``,
            existing directories are validated in-place and ``.zip`` archives
            use a deterministic content-hashed directory under the system
            temp root.
        prefix: Temporary directory prefix when ``target_dir`` is omitted.
        wallet_password: Optional passphrase used to decrypt ``ewallet.p12``
            when ``ewallet.pem`` is not present.

    Returns:
        Path to the prepared wallet directory.

    Raises:
        ImproperConfigurationError: If the wallet source is missing, corrupt,
            contains unsafe paths, cannot decrypt ``ewallet.p12``, or lacks
            required wallet artifacts.
    """
    if isinstance(wallet_zip, (str, Path)):
        candidate_dir = Path(wallet_zip)
        if candidate_dir.is_symlink():
            msg = f"Unsafe symlink for Oracle wallet directory: {candidate_dir}"
            raise ImproperConfigurationError(msg)
        if candidate_dir.is_dir():
            return _prepare_wallet_directory_source(
                candidate_dir.resolve(), target_dir=target_dir, prefix=prefix, wallet_password=wallet_password
            )

    if isinstance(wallet_zip, bytes):
        archive_bytes = wallet_zip
    else:
        zip_path = Path(wallet_zip)
        if not zip_path.is_file():
            msg = f"Oracle wallet archive does not exist: {zip_path}"
            raise ImproperConfigurationError(msg)
        archive_bytes = zip_path.read_bytes()

    if target_dir is None:
        digest = _compute_wallet_digest(archive_bytes, wallet_password)
        extract_dir = _default_temp_wallet_dir(prefix, digest)
        if _is_prepared_wallet_dir(extract_dir, check_owner=True):
            _track_atexit_wallet_dir(extract_dir)
            return extract_dir
        cleanup_on_error = not extract_dir.exists()
        _ensure_private_wallet_dir(extract_dir, check_owner=True)
        _track_atexit_wallet_dir(extract_dir)
    else:
        extract_dir = Path(target_dir)
        cleanup_on_error = False
        _ensure_private_wallet_dir(extract_dir, check_owner=False)

    resolved_root = extract_dir.resolve()
    try:
        _extract_wallet_bytes_into(archive_bytes, extract_dir, resolved_root, wallet_password=wallet_password)
    except BaseException:
        if cleanup_on_error:
            shutil.rmtree(extract_dir, ignore_errors=True)
        raise

    return extract_dir


def retain_extracted_wallet_dir(wallet_dir: Path) -> None:
    """Increment the active pool reference count for an extracted temporary wallet directory.

    Args:
        wallet_dir: Temporary wallet directory owned by one or more pool configurations.
    """
    resolved = wallet_dir.resolve()
    with _WALLET_DIR_LOCK:
        _WALLET_DIR_REFCOUNTS[resolved] = _WALLET_DIR_REFCOUNTS.get(resolved, 0) + 1
        _ATEXIT_WALLET_DIRS.add(resolved)
        _ensure_atexit_wallet_hook_locked()


def release_extracted_wallet_dir(wallet_dir: Path) -> None:
    """Decrement the active pool reference count and delete the wallet directory when unused.

    Args:
        wallet_dir: Temporary wallet directory previously retained by a pool configuration.
    """
    resolved = wallet_dir.resolve()
    should_delete = False
    with _WALLET_DIR_LOCK:
        remaining = _WALLET_DIR_REFCOUNTS.get(resolved, 0) - 1
        if remaining <= 0:
            _WALLET_DIR_REFCOUNTS.pop(resolved, None)
            _ATEXIT_WALLET_DIRS.discard(resolved)
            should_delete = True
        else:
            _WALLET_DIR_REFCOUNTS[resolved] = remaining
    if should_delete:
        shutil.rmtree(resolved, ignore_errors=True)


def parse_tnsnames_ora(source: "str | Path") -> "dict[str, str]":
    """Parse an Oracle ``tnsnames.ora`` file, directory, or text payload.

    Args:
        source: Path to ``tnsnames.ora``, path to a directory containing
            ``tnsnames.ora``, or raw ``tnsnames.ora`` file contents.

    Returns:
        Mapping of lowercase TNS service alias to normalized ``(DESCRIPTION=...)``
        connect descriptor string.
    """
    text = _read_ora_config_text(source, "tnsnames.ora")
    if not text:
        return {}
    cleaned_lines = [stripped.rstrip() for raw in text.splitlines() if (stripped := _strip_ora_comment(raw)).strip()]
    content = "\n".join(cleaned_lines)
    entries: dict[str, str] = {}
    idx = 0
    length = len(content)
    while idx < length:
        eq_pos = content.find("=", idx)
        if eq_pos == -1:
            break
        lhs = content[idx:eq_pos].strip()
        rhs_start = eq_pos + 1
        while rhs_start < length and content[rhs_start].isspace():
            rhs_start += 1
        if rhs_start >= length:
            break
        if content[rhs_start] != "(":
            line_end = content.find("\n", rhs_start)
            idx = length if line_end == -1 else line_end + 1
            continue
        pos = _scan_balanced_ora_parens(content, rhs_start)
        descriptor = " ".join(content[rhs_start:pos].split())
        for raw_alias in lhs.replace("\n", " ").split(","):
            alias = raw_alias.strip().lower()
            if alias:
                entries[alias] = descriptor
        idx = pos
    return entries


def parse_sqlnet_ora(source: "str | Path") -> "dict[str, str]":
    """Parse an Oracle ``sqlnet.ora`` file, directory, or text payload.

    Args:
        source: Path to ``sqlnet.ora``, path to a directory containing
            ``sqlnet.ora``, or raw ``sqlnet.ora`` file contents.

    Returns:
        Mapping of uppercase parameter name to string value.
    """
    text = _read_ora_config_text(source, "sqlnet.ora")
    if not text:
        return {}
    cleaned_lines = [stripped.rstrip() for raw in text.splitlines() if (stripped := _strip_ora_comment(raw)).strip()]
    content = "\n".join(cleaned_lines)
    settings: dict[str, str] = {}
    idx = 0
    length = len(content)
    while idx < length:
        eq_pos = content.find("=", idx)
        if eq_pos == -1:
            break
        raw_lhs = content[idx:eq_pos].strip()
        norm_key = raw_lhs.splitlines()[-1].strip().upper() if raw_lhs else ""
        rhs_start = eq_pos + 1
        while rhs_start < length and content[rhs_start].isspace():
            rhs_start += 1
        if rhs_start >= length:
            break
        if content[rhs_start] == "(":
            pos = _scan_balanced_ora_parens(content, rhs_start)
            norm_val = " ".join(content[rhs_start:pos].split())
            idx = pos
        else:
            line_end = content.find("\n", rhs_start)
            pos = length if line_end == -1 else line_end
            norm_val = content[rhs_start:pos].strip().strip('"').strip("'")
            idx = pos + 1
        if norm_key:
            settings[norm_key] = norm_val
    return settings


def build_tns_description(topology: "OracleTopologyConfig | Mapping[str, Any]") -> str:
    """Compile a multi-endpoint Oracle Net ``(DESCRIPTION=...)`` connect descriptor.

    Args:
        topology: :class:`OracleTopologyConfig` or mapping of topology options.

    Returns:
        Formatted TNS descriptor string.
    """
    resolved = OracleTopologyConfig(**topology) if isinstance(topology, Mapping) else topology
    if not resolved.endpoints:
        msg = "OracleTopologyConfig requires at least one endpoint"
        raise ImproperConfigurationError(msg)
    _validate_tns_token(resolved.service_name, "service_name")
    for ep in resolved.endpoints:
        _validate_tns_token(ep.host, "host")
    desc_parts: list[str] = []
    if resolved.sdu is not None:
        desc_parts.append(f"(SDU={resolved.sdu})")
    if resolved.retry_count is not None:
        desc_parts.append(f"(RETRY_COUNT={resolved.retry_count})")
    if resolved.retry_delay is not None:
        desc_parts.append(f"(RETRY_DELAY={resolved.retry_delay})")
    if resolved.transport_connect_timeout is not None:
        desc_parts.append(f"(TRANSPORT_CONNECT_TIMEOUT={resolved.transport_connect_timeout})")

    addr_parts: list[str] = []
    if resolved.load_balance is not None:
        addr_parts.append(f"(LOAD_BALANCE={'ON' if resolved.load_balance else 'OFF'})")
    if resolved.failover is not None:
        addr_parts.append(f"(FAILOVER={'ON' if resolved.failover else 'OFF'})")
    if resolved.source_route:
        addr_parts.append("(SOURCE_ROUTE=ON)")
    addr_parts.extend(
        f"(ADDRESS=(PROTOCOL={ep.protocol.upper()})(HOST={ep.host})(PORT={ep.port}))" for ep in resolved.endpoints
    )
    desc_parts.append(f"(ADDRESS_LIST={''.join(addr_parts)})")

    connect_parts: list[str] = [f"(SERVICE_NAME={resolved.service_name})"]
    if resolved.server is not None:
        _validate_tns_token(resolved.server, "server")
        connect_parts.append(f"(SERVER={resolved.server})")
    if resolved.failover_mode_type is not None or resolved.failover_mode_method is not None:
        fm_type = resolved.failover_mode_type or "SELECT"
        fm_method = resolved.failover_mode_method or "BASIC"
        _validate_tns_token(fm_type, "failover_mode_type")
        _validate_tns_token(fm_method, "failover_mode_method")
        fm_clauses = [f"(TYPE={fm_type})", f"(METHOD={fm_method})"]
        if resolved.failover_retries is not None:
            fm_clauses.append(f"(RETRIES={resolved.failover_retries})")
        if resolved.failover_delay is not None:
            fm_clauses.append(f"(DELAY={resolved.failover_delay})")
        connect_parts.append(f"(FAILOVER_MODE={''.join(fm_clauses)})")
    desc_parts.append(f"(CONNECT_DATA={''.join(connect_parts)})")
    return f"(DESCRIPTION={''.join(desc_parts)})"


def build_session_checkout_plsql(
    config: "OracleSessionContextConfig | Mapping[str, Any]",
) -> "tuple[str | None, dict[str, Any]]":
    """Compile checkout session context into a single PL/SQL block and bind parameters."""
    plsql, bind_params, _ = _build_session_checkout_plan(config)
    return plsql, bind_params


def build_session_context_plsql(
    config: "OracleSessionContextConfig | Mapping[str, Any]",
) -> "tuple[str | None, dict[str, Any]]":
    """Return checkout PL/SQL block and bind parameters for :class:`OracleSessionContextConfig`."""
    return build_session_checkout_plsql(config)


def build_session_release_plsql(
    config: "OracleSessionContextConfig | Mapping[str, Any]",
) -> "tuple[str | None, dict[str, Any]]":
    """Compile release reset PL/SQL block and bind parameters."""
    plsql, bind_params, _ = _build_session_release_plan(config)
    return plsql, bind_params


def build_session_reset_plsql(
    config: "OracleSessionContextConfig | Mapping[str, Any]",
) -> "tuple[str | None, dict[str, Any]]":
    """Return release reset PL/SQL block and bind parameters for :class:`OracleSessionContextConfig`."""
    return build_session_release_plsql(config)


def execute_session_checkout_sync(
    connection: Any, config: "OracleSessionContextConfig | Mapping[str, Any]", *, skip_alter_session: bool = False
) -> None:
    """Apply checkout state and execute the optional PL/SQL block."""
    plsql, binds = prepare_session_checkout_pipeline(connection, config, skip_alter_session=skip_alter_session)
    if plsql is not None:
        with connection.cursor() as cursor:
            cursor.execute(plsql, binds) if binds else cursor.execute(plsql)


def execute_session_release_sync(connection: Any, config: OracleSessionContextConfig | Mapping[str, Any]) -> None:
    """Clear checkout state before making a connection available for reuse."""
    plsql, binds, deferred = prepare_session_release_pipeline(connection, config)
    if plsql is not None:
        with connection.cursor() as cursor:
            cursor.execute(plsql, binds) if binds else cursor.execute(plsql)
    if deferred is not None:
        deferred()


async def execute_session_checkout_async(
    connection: Any, config: "OracleSessionContextConfig | Mapping[str, Any]", *, skip_alter_session: bool = False
) -> None:
    """Apply asynchronous checkout state and optional PL/SQL block."""
    plsql, binds = prepare_session_checkout_pipeline(connection, config, skip_alter_session=skip_alter_session)
    if plsql is not None:
        with connection.cursor() as cursor:
            await cursor.execute(plsql, binds) if binds else await cursor.execute(plsql)


async def execute_session_release_async(
    connection: Any, config: OracleSessionContextConfig | Mapping[str, Any]
) -> None:
    """Clear asynchronous session state before pool return."""
    plsql, binds, deferred = prepare_session_release_pipeline(connection, config)
    if plsql is not None:
        with connection.cursor() as cursor:
            await cursor.execute(plsql, binds) if binds else await cursor.execute(plsql)
    if deferred is not None:
        deferred()


def sanitize_oracle_text_query(raw_query: str, *, operator: str = "ACCUM") -> str:
    """Sanitize a free-text search string for Oracle Text ``CONTAINS``.

    Extracts alphanumeric tokens and wraps each token in ``{token}`` literal
    braces so user queries containing Oracle Text reserved keywords or syntax
    punctuation never raise ``DRG-50901``. When ``operator="NEAR"``, multi-token
    queries are formatted using functional ``NEAR(({tok1}, {tok2}, ...))`` syntax.

    Args:
        raw_query: Raw user search string.
        operator: Operator used to join escaped tokens (``"ACCUM"``, ``"AND"``,
            ``"OR"``, or ``"NEAR"``, case-insensitive; defaults to ``"ACCUM"``).

    Returns:
        Sanitized Oracle Text query string, or ``""`` when no tokens remain.

    Raises:
        ImproperConfigurationError: If ``operator`` is not a supported Oracle Text operator.
    """
    normalized_op = operator.strip().upper()
    if normalized_op not in _ORACLE_TEXT_OPERATORS:
        msg = f"Unsupported Oracle Text operator: {operator!r}"
        raise ImproperConfigurationError(msg)
    if not raw_query or not raw_query.strip():
        return ""
    tokens = [match.group(0) for match in _ORACLE_TEXT_TOKEN_PATTERN.finditer(raw_query)]
    if not tokens:
        return ""
    escaped = [f"{{{tok}}}" for tok in tokens]
    if normalized_op == "NEAR":
        if len(escaped) == 1:
            return escaped[0]
        return f"NEAR(({', '.join(escaped)}))"
    return f" {normalized_op} ".join(escaped)


def extract_duality_view_etag(row_or_doc: "Mapping[str, Any] | str | bytes") -> str | None:
    """Extract ``_metadata.etag`` from an Oracle 26ai JSON Relational Duality View document or row.

    Args:
        row_or_doc: Duality view document mapping, row mapping containing a
            ``data``/``DATA`` document, or raw JSON string/bytes.

    Returns:
        The hex ETag string if present, otherwise ``None``.
    """
    parsed: Any = row_or_doc
    if isinstance(parsed, (str, bytes)):
        try:
            parsed = from_json(parsed)
        except (ValueError, TypeError):
            return None
    if not isinstance(parsed, Mapping):
        return None
    metadata = parsed.get("_metadata")
    if isinstance(metadata, Mapping):
        etag = metadata.get("etag")
        if isinstance(etag, str) and etag:
            return etag
    for key in ("data", "DATA"):
        nested = parsed.get(key)
        if isinstance(nested, (str, bytes)):
            try:
                nested = from_json(nested)
            except (ValueError, TypeError):
                continue
        if isinstance(nested, Mapping):
            nested_meta = nested.get("_metadata")
            if isinstance(nested_meta, Mapping):
                etag = nested_meta.get("etag")
                if isinstance(etag, str) and etag:
                    return etag
    return None


def extract_duality_etag(payload: "Mapping[str, Any] | str | bytes") -> str | None:
    """Alias for :func:`extract_duality_view_etag`."""
    return extract_duality_view_etag(payload)


def inject_duality_view_etag(doc: "Mapping[str, Any]", etag: str) -> "dict[str, Any]":
    """Return a copy of a Duality View document with ``_metadata.etag`` injected for optimistic concurrency."""
    result: dict[str, Any] = dict(doc)
    existing_meta = result.get("_metadata")
    merged_meta: dict[str, Any] = dict(existing_meta) if isinstance(existing_meta, Mapping) else {}
    merged_meta["etag"] = etag
    result["_metadata"] = merged_meta
    return result


def build_duality_etag_update_sql(
    view_name: str, id_column: str = "ID", data_column: str = "DATA", *, etag_param: str = "expected_etag"
) -> str:
    """Build an optimistic-concurrency ``UPDATE`` statement for an Oracle 26ai JSON Relational Duality View."""
    _validate_oracle_identifier(view_name, "view_name")
    _validate_oracle_identifier(id_column, "id_column")
    _validate_oracle_identifier(data_column, "data_column")
    _validate_oracle_identifier(etag_param, "etag_param")
    return (
        f"UPDATE {view_name} SET {data_column} = :data "
        f"WHERE {id_column} = :id AND JSON_VALUE({data_column}, '$._metadata.etag') = :{etag_param}"
    )


def is_stateful_oracle_session_context(config: OracleSessionContextConfig | Mapping[str, Any] | None) -> bool:
    """Identify server session state that cannot survive statement-boundary pooling."""
    if config is None:
        return False
    fields = (
        "client_identifier",
        "clear_identifier",
        "edition",
        "time_zone",
        "nls_parameters",
        "contexts",
        "context_attributes",
        "clear_context_namespaces",
        "ebs_apps_initialize",
        "mo_global_policy",
        "apex_session",
        "apex_detach",
        "reset_package",
        "current_schema",
        "end_user_security",
        "transaction_priority",
    )
    return any(
        bool(config.get(name) if isinstance(config, Mapping) else getattr(config, name, None)) for name in fields
    )


def validate_oracle_drcp_and_mode_constraints(
    connection_config: Mapping[str, Any], driver_features: Mapping[str, Any] | None = None
) -> dict[str, Any]:
    """Reject DRCP and edition combinations that lose state or conflict in the SDK."""
    config = dict(connection_config)
    features = driver_features or {}
    extra = config.get("extra")
    values = dict(extra) if isinstance(extra, Mapping) else {}
    values.update(config)
    dsn = str(config.get("dsn", ""))
    for key in ("pool_boundary", "pool_name", "cclass", "edition"):
        match = re.search(r"(?:[?&]|\()" + key + r"\s*=\s*([^)&]+)", dsn, re.IGNORECASE)
        if key not in values and match:
            values[key] = match.group(1)
    boundary = values.get("pool_boundary")
    if boundary is not None:
        if not isinstance(boundary, str) or boundary.lower() not in {"statement", "transaction"}:
            msg = "Oracle pool_boundary must be 'statement' or 'transaction'"
            raise ImproperConfigurationError(msg)
        if "pool_boundary" in config:
            config["pool_boundary"] = boundary.lower()
        if boundary.lower() == "statement" and (
            features.get("tenant_strategy")
            or is_stateful_oracle_session_context(features.get("session_context_hook"))
            or values.get("transaction_priority")
            or values.get("edition")
        ):
            msg = "Statement-boundary DRCP cannot preserve tenant or stateful session settings"
            raise ImproperConfigurationError(msg)
    if values.get("edition") is not None and values.get("cclass") is not None:
        msg = "Oracle edition and cclass conflict; set edition through OracleSessionContextConfig instead"
        raise ImproperConfigurationError(msg)
    hook = features.get("session_context_hook")
    security = hook.get("end_user_security") if isinstance(hook, Mapping) else getattr(hook, "end_user_security", None)
    if security is not None:
        thick = (
            bool(config.get("thick_mode"))
            or config.get("lib_dir") is not None
            or bool(config.get("soda_metadata_cache"))
            or not oracledb_module.is_thin_mode()
        )
        plain_tcp = (
            str(values.get("protocol", "")).lower() == "tcp"
            or dsn.lower().startswith("tcp://")
            or bool(re.search(r"\(PROTOCOL\s*=\s*TCP\)", dsn, re.IGNORECASE))
        )
        if thick or plain_tcp:
            msg = "Oracle Deep Data Security requires Thin mode over TCPS"
            raise ImproperConfigurationError(msg)
    if values.get("pool_name") is not None:
        name = values["pool_name"]
        if not isinstance(name, str) or not re.fullmatch(r"[A-Za-z0-9_$#.-]+", name):
            msg = "Oracle pool_name must be a nonempty TNS-safe token"
            raise ImproperConfigurationError(msg)
    return config


def normalize_oracle_purity(purity: Any) -> int | None:
    """Normalize native purity enums, valid integer constants, and named inputs."""
    if purity is None:
        return None
    if isinstance(purity, str):
        purity = getattr(oracledb_module.Purity, purity.upper(), None)
        if purity is None:
            msg = "Oracle purity must be DEFAULT, NEW, or SELF"
            raise ImproperConfigurationError(msg)
    if isinstance(purity, bool) or not isinstance(purity, int) or purity not in tuple(oracledb_module.Purity):
        msg = "Invalid Oracle session purity"
        raise ImproperConfigurationError(msg)
    return int(purity)


def normalize_oracle_sharding_keys(
    sharding_key: Sequence[Any] | None, super_sharding_key: Sequence[Any] | None = None
) -> tuple[list[Any] | None, list[Any] | None]:
    """Normalize nonempty shard sequences and reject orphan super-shard keys."""
    normalized: list[list[Any] | None] = []
    for value in (sharding_key, super_sharding_key):
        if value is not None and (isinstance(value, (str, bytes)) or not isinstance(value, Sequence) or not value):
            msg = "Oracle sharding keys must be nonempty sequences, not strings or bytes"
            raise ImproperConfigurationError(msg)
        normalized.append(list(value) if value is not None else None)
    if normalized[1] is not None and normalized[0] is None:
        msg = "Oracle super_sharding_key requires sharding_key"
        raise ImproperConfigurationError(msg)
    return normalized[0], normalized[1]


def normalize_oracle_transaction_priority(value: Any) -> Any:
    """Resolve optional native transaction priority without increasing the SDK floor."""
    if value is None:
        return None
    priority_type = getattr(oracledb_module, "TransactionPriority", None)
    if priority_type is None:
        msg = "Oracle transaction priority requires python-oracledb 26.1 or newer"
        raise oracledb_module.NotSupportedError(msg)
    if isinstance(value, str):
        value = getattr(priority_type, value.upper(), None)
    if value is None or isinstance(value, bool) or not isinstance(value, priority_type):
        msg = "Oracle transaction priority must be DEFAULT, LOW, MEDIUM, or HIGH"
        raise ImproperConfigurationError(msg)
    return value


def apply_oracle_transaction_priority(connection: Any, priority: Any) -> None:
    """Assign native priority and map optional-feature server rejection."""
    resolved = normalize_oracle_transaction_priority(priority)
    if resolved is not None:
        try:
            connection.transaction_priority = resolved
        except oracledb_module.Error as exc:
            raise create_mapped_exception(exc) from exc


class OracleAcquireOptions:
    """Normalized Oracle checkout overrides with stable equality and hash semantics."""

    __slots__ = (
        "cclass",
        "match_any_tag",
        "purity",
        "sharding_key",
        "super_sharding_key",
        "tag",
        "transaction_priority",
    )

    def __init__(
        self,
        *,
        sharding_key: Sequence[Any] | None = None,
        super_sharding_key: Sequence[Any] | None = None,
        tag: str | None = None,
        match_any_tag: bool = False,
        cclass: str | None = None,
        purity: Any = None,
        transaction_priority: Any = None,
    ) -> None:
        self.sharding_key, self.super_sharding_key = normalize_oracle_sharding_keys(sharding_key, super_sharding_key)
        self.tag = tag
        self.match_any_tag = match_any_tag
        self.cclass = cclass
        self.purity = normalize_oracle_purity(purity)
        self.transaction_priority = normalize_oracle_transaction_priority(transaction_priority)

    def is_default(self) -> bool:
        """Return whether checkout can use native pool defaults without kwargs."""
        return (
            all(
                value is None
                for value in (
                    self.sharding_key,
                    self.super_sharding_key,
                    self.tag,
                    self.cclass,
                    self.purity,
                    self.transaction_priority,
                )
            )
            and not self.match_any_tag
        )

    def __repr__(self) -> str:
        return f"OracleAcquireOptions(sharding_key={self.sharding_key!r}, super_sharding_key={self.super_sharding_key!r}, tag={self.tag!r}, match_any_tag={self.match_any_tag!r}, cclass={self.cclass!r}, purity={self.purity!r}, transaction_priority={self.transaction_priority!r})"

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, OracleAcquireOptions):
            return NotImplemented
        return (
            self.sharding_key,
            self.super_sharding_key,
            self.tag,
            self.match_any_tag,
            self.cclass,
            self.purity,
            self.transaction_priority,
        ) == (
            other.sharding_key,
            other.super_sharding_key,
            other.tag,
            other.match_any_tag,
            other.cclass,
            other.purity,
            other.transaction_priority,
        )

    def __hash__(self) -> int:
        return hash((
            tuple(self.sharding_key) if self.sharding_key is not None else None,
            tuple(self.super_sharding_key) if self.super_sharding_key is not None else None,
            self.tag,
            self.match_any_tag,
            self.cclass,
            self.purity,
            self.transaction_priority,
        ))


def _oracle_context_value(config: Any, field: str) -> Any:
    return config.get(field) if isinstance(config, Mapping) else getattr(config, field, None)


def synthesize_session_state_tag(
    config: OracleSessionContextConfig | Mapping[str, Any] | None, *, explicit_tag: str | None = None
) -> str | None:
    """Build a deterministic OCI state tag without evaluating dynamic session fields."""
    pairs: dict[str, str] = {}
    for field in ("edition", "time_zone", "current_schema", "standby_max_data_delay", "adg_redirect_dml"):
        value = _oracle_context_value(config, field)
        if value is None or callable(value):
            continue
        if field == "standby_max_data_delay" and (isinstance(value, bool) or not isinstance(value, int)):
            msg = "standby_max_data_delay must be an integer"
            raise ImproperConfigurationError(msg)
        pairs[field.upper()] = str(value).upper() if isinstance(value, bool) else str(value)
    nls = _oracle_context_value(config, "nls_parameters")
    if isinstance(nls, Mapping):
        _validate_oracle_nls_parameters(nls)
        pairs.update({str(key).upper(): str(value) for key, value in nls.items()})
    if explicit_tag is not None:
        for segment in explicit_tag.split(";"):
            key, separator, value = segment.partition("=")
            if not separator or not key or not value or any(char in segment for char in "\r\n"):
                msg = "Oracle state tag must contain nonempty KEY=VALUE segments"
                raise ImproperConfigurationError(msg)
            pairs[key] = value
    for key, value in pairs.items():
        if any(char in key + value for char in ";\r\n") or "=" in key:
            msg = "Oracle state tag components cannot contain delimiters or line breaks"
            raise ImproperConfigurationError(msg)
    return ";".join(f"{key}={pairs[key]}" for key in sorted(pairs)) or None


def validate_oracle_session_callback(callback: Any, *, is_thin_mode: bool) -> Callable[..., Any] | str | None:
    """Validate a native callback or qualified PL/SQL procedure before pool creation."""
    if callback is None or callable(callback):
        return cast("Callable[..., Any] | None", callback)
    if not isinstance(callback, str) or not re.fullmatch(
        r"[A-Za-z][A-Za-z0-9_$#]*(?:\.[A-Za-z][A-Za-z0-9_$#]*){0,2}", callback
    ):
        msg = "Oracle session_callback must be a callable or qualified PL/SQL procedure"
        raise ImproperConfigurationError(msg)
    if is_thin_mode:
        msg = "PL/SQL session_callback requires Oracle Thick mode"
        raise ImproperConfigurationError(msg)
    return callback


def extract_static_alter_session_config(
    config: OracleSessionContextConfig | Mapping[str, Any] | None,
) -> OracleSessionContextConfig | None:
    """Select static ALTER SESSION fields for one-time owned Thin connection setup."""
    values = {field: _oracle_context_value(config, field) for field in ("edition", "time_zone", "nls_parameters")}
    static = {field: value for field, value in values.items() if value is not None and not callable(value)}
    return OracleSessionContextConfig(**static) if static else None


def has_callable_alter_session_fields(config: OracleSessionContextConfig | Mapping[str, Any] | None) -> bool:
    """Check whether ALTER SESSION settings must be resolved for every checkout."""
    return any(callable(_oracle_context_value(config, field)) for field in ("edition", "time_zone", "nls_parameters"))


def normalize_oracle_config_dsn(dsn: str) -> str:
    """Normalize file and centralized configuration URI schemes."""
    scheme, separator, payload = dsn.partition("://")
    normalized = scheme.lower()
    if not separator or (normalized != "file" and not normalized.startswith("config-")):
        return dsn
    if normalized in {"config-aws", "config-gcp"}:
        alternatives = (
            "config-awss3:// or config-awssecretsmanager://"
            if normalized == "config-aws"
            else "config-gcpstorage:// or config-gcpsecretmanager://"
        )
        msg = f"Invalid Oracle config provider {scheme!r}; use {alternatives}"
        raise ImproperConfigurationError(msg)
    if not payload:
        msg = "Oracle configuration URI requires a nonempty payload"
        raise ImproperConfigurationError(msg)
    if normalized == "file":
        normalized = "config-file"
        if payload.startswith("localhost/"):
            payload = payload[len("localhost") :]
    return f"{normalized}://{payload}"


def build_connection_config(connection_config: "Mapping[str, Any]") -> "dict[str, Any]":
    """Build a normalized connection configuration dictionary.

    Args:
        connection_config: Raw connection configuration mapping.

    Returns:
        Normalized connection parameters.
    """
    config = {k: v for k, v in connection_config.items() if v is not None}
    if "wallet_zip" in config:
        wallet_dir = extract_oracle_wallet_zip(config.pop("wallet_zip"), wallet_password=config.get("wallet_password"))
        config.setdefault("config_dir", str(wallet_dir))
        config.setdefault("wallet_location", str(wallet_dir))
    tns_alias = config.pop("tns_alias", None)
    if "topology" in config:
        topology_val = config.pop("topology")
        resolved_topology = OracleTopologyConfig(**topology_val) if isinstance(topology_val, Mapping) else topology_val
        if "dsn" not in config:
            config["dsn"] = build_tns_description(resolved_topology)
        if resolved_topology.retry_count is not None and "retry_count" not in config:
            config["retry_count"] = resolved_topology.retry_count
        if resolved_topology.retry_delay is not None and "retry_delay" not in config:
            config["retry_delay"] = resolved_topology.retry_delay
    if "dsn" not in config:
        dsn = config.pop("url", None) or config.pop("connection_string", None)
        if dsn is not None:
            config["dsn"] = dsn
    else:
        config.pop("url", None)
        config.pop("connection_string", None)
    user_alias = config.pop("username", None)
    if "user" not in config and user_alias is not None:
        config["user"] = user_alias
    if isinstance(config.get("dsn"), str):
        config["dsn"] = normalize_oracle_config_dsn(config["dsn"])
    shard = config.pop("sharding_key", None)
    super_shard = config.pop("super_sharding_key", None)
    if (shard is not None and "shardingkey" in config) or (super_shard is not None and "supershardingkey" in config):
        msg = "Oracle sharding aliases cannot be specified together"
        raise ImproperConfigurationError(msg)
    shard, super_shard = normalize_oracle_sharding_keys(
        shard if shard is not None else config.get("shardingkey"),
        super_shard if super_shard is not None else config.get("supershardingkey"),
    )
    if shard is not None:
        config["shardingkey"] = shard
    if super_shard is not None:
        config["supershardingkey"] = super_shard
    if "transaction_priority" in config:
        config["transaction_priority"] = normalize_oracle_transaction_priority(config["transaction_priority"])
    if "appcontext" in config:
        config["appcontext"] = normalize_oracle_appcontext(config["appcontext"])
    _apply_ora_directory_resolution(config, tns_alias=tns_alias)
    dsn = config.get("dsn", "")
    if (
        isinstance(dsn, str)
        and dsn.startswith("config-")
        and "://" in dsn
        and (
            bool(config.get("thick_mode"))
            or config.get("lib_dir") is not None
            or bool(config.get("soda_metadata_cache"))
            or not oracledb_module.is_thin_mode()
        )
    ):
        config.setdefault("thick_mode_dsn_passthrough", False)
    return config


def resolve_dbms_cloud_source_uri(source: "str | Path", *, file_format: str) -> str | None:
    """Return a normalized HTTP(S) object-store URI when eligible for ``DBMS_CLOUD.COPY_DATA``.

    Oracle ``DBMS_CLOUD.COPY_DATA`` requires HTTP(S) REST endpoints in
    ``file_uri_list``. Direct ``https://`` and ``http://`` URLs are returned
    verbatim; ``s3://``, ``gs://`` / ``gcs://``, ``wasbs://`` / ``wasb://``,
    and ``abfss://`` / ``abfs://`` URIs with deterministic REST host mappings
    are normalized to HTTP(S); and local paths or schemes lacking host/region
    metadata (such as ``oci://`` or ``az://``) return ``None`` so callers fall
    back to client-side Arrow ingestion.

    Args:
        source: Storage destination string or local path.
        file_format: Requested storage format name.

    Returns:
        Normalized HTTP(S) URI string when ``file_format`` and URI scheme are
        supported by ``DBMS_CLOUD.COPY_DATA``, otherwise ``None``.
    """
    if file_format.strip().lower() not in _DBMS_CLOUD_FORMAT_MAP:
        return None
    if not isinstance(source, str):
        return None
    cleaned = source.strip()
    if "://" not in cleaned:
        return None
    raw_scheme, _, rest = cleaned.partition("://")
    scheme = raw_scheme.strip().lower()
    if not rest:
        return None
    if scheme in {"https", "http"}:
        host, sep, obj_path = rest.partition("/")
        return cleaned if host and sep and obj_path else None
    if scheme in {"s3", "s3a", "s3n"}:
        bucket, sep, key = rest.partition("/")
        if not bucket or not sep or not key:
            return None
        if "." in bucket:
            return f"https://s3.amazonaws.com/{bucket}/{key}"
        return f"https://{bucket}.s3.amazonaws.com/{key}"
    if scheme in {"gs", "gcs"}:
        bucket, sep, key = rest.partition("/")
        if not bucket or not sep or not key:
            return None
        if "." in bucket:
            return f"https://storage.googleapis.com/{bucket}/{key}"
        return f"https://{bucket}.storage.googleapis.com/{key}"
    if scheme in {"wasbs", "wasb", "abfss", "abfs"}:
        authority, sep, key = rest.partition("/")
        if not sep or not key or "@" not in authority:
            return None
        container, _, host = authority.partition("@")
        if not container or not host or "." not in host:
            return None
        blob_host = (
            host.replace(".dfs.core.windows.net", ".blob.core.windows.net") if scheme in {"abfss", "abfs"} else host
        )
        proto = "http" if scheme == "wasb" else "https"
        return f"{proto}://{blob_host}/{container}/{key}"
    return None


def build_dbms_cloud_format_json(file_format: str, format_options: "Mapping[str, Any] | None" = None) -> str:
    """Serialize the ``format`` JSON parameter for ``DBMS_CLOUD.COPY_DATA``.

    Args:
        file_format: Storage format (``"parquet"``, ``"csv"``, ``"json"``,
            ``"jsonl"``, or ``"ndjson"``).
        format_options: Optional ``DBMS_CLOUD`` format attributes merged over defaults.

    Returns:
        JSON string suitable for the ``format`` argument of ``DBMS_CLOUD.COPY_DATA``.

    Raises:
        ImproperConfigurationError: If ``file_format`` is not supported by ``DBMS_CLOUD.COPY_DATA``.
    """
    norm_format = file_format.strip().lower()
    cloud_type = _DBMS_CLOUD_FORMAT_MAP.get(norm_format)
    if cloud_type is None:
        msg = f"Unsupported DBMS_CLOUD.COPY_DATA format: {file_format!r}"
        raise ImproperConfigurationError(msg)
    fmt_payload: dict[str, Any] = {"type": cloud_type}
    if cloud_type == "csv":
        fmt_payload["skipheaders"] = 1
    if format_options:
        fmt_payload.update(format_options)
    return to_json(fmt_payload)


def build_dbms_cloud_copy_data_sql(
    table: str,
    *,
    credential_name: str,
    file_uri: str,
    file_format: str,
    default_schema: str | None = None,
    format_options: "Mapping[str, Any] | None" = None,
) -> "tuple[str, dict[str, Any]]":
    """Build a parameterized ``DBMS_CLOUD.COPY_DATA`` PL/SQL block and bind dictionary.

    Preserves double quotes on case-sensitive or special-character Oracle
    identifiers so ``DBMS_CLOUD.COPY_DATA`` targets the exact same table and
    schema as ``TRUNCATE TABLE`` when ``overwrite=True``.

    Args:
        table: Qualified or unqualified target table identifier.
        credential_name: ``DBMS_CLOUD`` credential name (e.g. ``"OCI$RESOURCE_PRINCIPAL"``).
        file_uri: Remote HTTP(S) object-store URI passed to ``file_uri_list``.
        file_format: Storage format (``"parquet"``, ``"csv"``, ``"json"``, ``"jsonl"``, ``"ndjson"``).
        default_schema: Optional default schema name when ``table`` is unqualified.
        format_options: Optional ``DBMS_CLOUD`` format attributes merged over defaults.

    Returns:
        Tuple of ``(plsql_block, bind_params)``.

    Raises:
        SQLSpecError: If ``table`` or ``default_schema`` is empty or invalid.
        ImproperConfigurationError: If ``credential_name``, ``file_uri``, or ``file_format`` is invalid.
    """
    schema_name, table_name = _split_oracle_dbms_cloud_table(table)
    if schema_name is None and isinstance(default_schema, str) and default_schema.strip():
        schema_name = _normalize_oracle_dbms_cloud_part(default_schema, full_identifier=default_schema)
    cleaned_credential = credential_name.strip()
    if not cleaned_credential:
        msg = "DBMS_CLOUD.COPY_DATA requires a non-empty credential_name"
        raise ImproperConfigurationError(msg)
    cleaned_uri = file_uri.strip()
    if not cleaned_uri:
        msg = "DBMS_CLOUD.COPY_DATA requires a non-empty file_uri"
        raise ImproperConfigurationError(msg)
    format_json = build_dbms_cloud_format_json(file_format, format_options)
    binds: dict[str, Any] = {
        "table_name": table_name,
        "credential_name": cleaned_credential,
        "file_uri_list": cleaned_uri,
        "format_json": format_json,
    }
    if schema_name:
        binds["schema_name"] = schema_name
        plsql = (
            "BEGIN "
            "DBMS_CLOUD.COPY_DATA("
            "table_name => :table_name, "
            "credential_name => :credential_name, "
            "file_uri_list => :file_uri_list, "
            "schema_name => :schema_name, "
            "format => :format_json"
            "); "
            "END;"
        )
    else:
        plsql = (
            "BEGIN "
            "DBMS_CLOUD.COPY_DATA("
            "table_name => :table_name, "
            "credential_name => :credential_name, "
            "file_uri_list => :file_uri_list, "
            "format => :format_json"
            "); "
            "END;"
        )
    return plsql, binds


def apply_wallet_ssl_context(config: dict[str, Any]) -> None:
    """Apply an SSLContext for Oracle wallets with non-strict self-signed CAs if needed.

    Args:
        config: Connection configuration dictionary.
    """
    if oracledb_module.is_thin_mode() and "ssl_context" not in config:
        wallet_loc = config.get("wallet_location") or config.get("config_dir")
        wallet_ssl_ctx = build_wallet_ssl_context_if_needed(wallet_loc)
        if wallet_ssl_ctx is not None:
            config["ssl_context"] = wallet_ssl_ctx


def build_wallet_ssl_context_if_needed(wallet_location: Any) -> "ssl.SSLContext | None":
    """Build a Thin-mode ``ssl.SSLContext`` for Oracle wallets with non-strict self-signed CAs.

    Oracle ``orapki -self_signed`` wallets (including Autonomous Database Free container
    wallets) emit X.509 v1 or extension-less self-signed trust-anchor certificates that
    OpenSSL rejects when Python 3.13+ enables ``ssl.VERIFY_X509_STRICT`` by default.
    RFC 5280 X.509 v3 CA wallets with ``BasicConstraints(ca=True)`` return ``None`` so
    the runtime's default strict verification remains intact.

    Args:
        wallet_location: Configured Oracle wallet directory path, if any.

    Returns:
        An ``ssl.SSLContext`` with ``VERIFY_X509_STRICT`` cleared while preserving
        ``ssl.CERT_REQUIRED``, or ``None`` when the default ``ssl.SSLContext`` is compatible.
    """
    if not wallet_location:
        return None
    location_str = str(wallet_location).strip()
    if not location_str or location_str.upper() == "SYSTEM":
        return None
    pem_path = Path(location_str) / "ewallet.pem"
    try:
        if pem_path.is_symlink() or not pem_path.is_file():
            return None
    except OSError:
        return None
    if not _wallet_pem_has_non_strict_ca(pem_path):
        return None
    strict_flag = ssl.VERIFY_X509_STRICT
    ctx = ssl.create_default_context()
    if not (ctx.verify_flags & strict_flag):
        return None
    ctx.verify_flags &= ~strict_flag
    return ctx


def _build_session_checkout_plan(
    config: "OracleSessionContextConfig | Mapping[str, Any]", *, skip_alter_session: bool = False
) -> "tuple[str | None, dict[str, Any], dict[str, str | None]]":
    resolved = OracleSessionContextConfig(**config) if isinstance(config, Mapping) else config
    piggyback_attrs: dict[str, str | None] = {}

    client_id = _resolve_callable_or_str(resolved.client_identifier)
    if client_id is not None:
        piggyback_attrs["client_identifier"] = client_id
    mod = _resolve_callable_or_str(resolved.module)
    if mod is not None:
        piggyback_attrs["module"] = mod
    act = _resolve_callable_or_str(resolved.action)
    if act is not None:
        piggyback_attrs["action"] = act
    ecid_val = _resolve_callable_or_str(resolved.ecid)
    if ecid_val is not None:
        piggyback_attrs["econtext_id"] = ecid_val

    for field in ("clientinfo", "dbop", "current_schema"):
        value = _resolve_callable_or_str(getattr(resolved, field))
        if value is not None:
            if field == "current_schema":
                _validate_oracle_identifier(value, "current_schema")
            piggyback_attrs[field] = value

    alter_clauses: list[str] = []
    tz = _resolve_callable_or_str(resolved.time_zone)
    if tz is not None:
        _validate_oracle_time_zone(tz)
        alter_clauses.append(f"TIME_ZONE = ''{tz}''")

    ed = _resolve_callable_or_str(resolved.edition)
    if ed is not None:
        _validate_oracle_identifier(ed, "edition")
        alter_clauses.append(f"EDITION = {ed}")

    nls_raw = resolved.nls_parameters() if callable(resolved.nls_parameters) else resolved.nls_parameters
    if nls_raw:
        _validate_oracle_nls_parameters(nls_raw)
        for key in sorted(nls_raw.keys()):
            val = nls_raw[key]
            alter_clauses.append(f"{key.upper()} = ''{val}''")

    plsql_statements: list[str] = []
    if alter_clauses and not skip_alter_session:
        joined_alter = " ".join(alter_clauses)
        plsql_statements.append(f"EXECUTE IMMEDIATE 'ALTER SESSION SET {joined_alter}';")

    bind_params: dict[str, Any] = {}
    uses_plsql_identifier = bool(
        resolved.clear_identifier
        or resolved.ebs_apps_initialize is not None
        or resolved.mo_global_policy is not None
        or resolved.apex_session is not None
        or (
            client_id is not None
            and resolved.module is None
            and resolved.action is None
            and resolved.ecid is None
            and not alter_clauses
            and not resolved.contexts
        )
    )
    if client_id is not None and uses_plsql_identifier:
        plsql_statements.append("DBMS_SESSION.SET_IDENTIFIER(:p_client_id);")
        bind_params["p_client_id"] = client_id

    ctx_raw = resolved.contexts() if callable(resolved.contexts) else resolved.contexts
    if ctx_raw:
        _validate_oracle_contexts(ctx_raw)
        idx = 0
        for namespace in sorted(ctx_raw.keys()):
            if namespace.upper() == "CLIENTCONTEXT":
                continue
            attrs = ctx_raw[namespace]
            for attr_key in sorted(attrs.keys()):
                raw_val = attrs[attr_key]
                attr_val = _resolve_callable_or_str(raw_val)
                ns_param = f"ctx_ns_{idx}"
                key_param = f"ctx_attr_{idx}"
                val_param = f"ctx_val_{idx}"
                bind_params[ns_param] = namespace
                bind_params[key_param] = attr_key
                bind_params[val_param] = attr_val
                plsql_statements.append(f"DBMS_SESSION.SET_CONTEXT(:{ns_param}, :{key_param}, :{val_param});")
                idx += 1

    if resolved.ebs_apps_initialize is not None:
        u_id, r_id, a_id = resolved.ebs_apps_initialize
        plsql_statements.append(
            "FND_GLOBAL.APPS_INITIALIZE(user_id => :p_ebs_uid, resp_id => :p_ebs_rid, resp_appl_id => :p_ebs_aid);"
        )
        bind_params["p_ebs_uid"] = u_id
        bind_params["p_ebs_rid"] = r_id
        bind_params["p_ebs_aid"] = a_id

    if resolved.mo_global_policy is not None:
        mode, org_id = resolved.mo_global_policy
        plsql_statements.append("MO_GLOBAL.SET_POLICY_CONTEXT(:p_mo_mode, :p_mo_org);")
        bind_params["p_mo_mode"] = mode
        bind_params["p_mo_org"] = org_id

    if resolved.apex_session is not None:
        app_id, page_id, sess_id = resolved.apex_session
        plsql_statements.append(
            "APEX_SESSION.ATTACH(p_app_id => :p_apex_app, p_page_id => :p_apex_page, p_session_id => :p_apex_sess);"
        )
        bind_params["p_apex_app"] = app_id
        bind_params["p_apex_page"] = page_id
        bind_params["p_apex_sess"] = sess_id

    if not plsql_statements:
        return None, bind_params, piggyback_attrs
    return f"BEGIN {' '.join(plsql_statements)} END;", bind_params, piggyback_attrs


def _build_session_release_plan(
    config: "OracleSessionContextConfig | Mapping[str, Any]",
) -> "tuple[str | None, dict[str, Any], dict[str, str | None]]":
    resolved = OracleSessionContextConfig(**config) if isinstance(config, Mapping) else config
    if not resolved.reset_on_release and not (
        resolved.apex_detach or resolved.clear_identifier or resolved.clear_context_namespaces or resolved.reset_package
    ):
        return None, {}, {}

    piggyback_attrs: dict[str, str | None] = {}
    if resolved.client_identifier is not None or resolved.clear_identifier:
        piggyback_attrs["client_identifier"] = None
    if resolved.module is not None:
        piggyback_attrs["module"] = ""
    if resolved.action is not None:
        piggyback_attrs["action"] = ""
    if resolved.ecid is not None:
        piggyback_attrs["econtext_id"] = ""
    if resolved.clientinfo is not None:
        piggyback_attrs["clientinfo"] = ""
    if resolved.dbop is not None:
        piggyback_attrs["dbop"] = ""

    plsql_statements: list[str] = []
    bind_params: dict[str, Any] = {}
    if resolved.apex_detach:
        plsql_statements.append("APEX_SESSION.DETACH;")
    if resolved.clear_identifier:
        plsql_statements.append("DBMS_SESSION.CLEAR_IDENTIFIER;")
    idx = 0
    for namespace in resolved.clear_context_namespaces:
        if namespace.upper() == "CLIENTCONTEXT":
            continue
        plsql_statements.append(f"DBMS_SESSION.CLEAR_ALL_CONTEXT(:p_clr_ns_{idx});")
        bind_params[f"p_clr_ns_{idx}"] = namespace
        idx += 1
    if resolved.reset_on_release:
        ctx_raw = resolved.contexts() if callable(resolved.contexts) else resolved.contexts
        if ctx_raw:
            _validate_oracle_contexts(ctx_raw)
            for namespace in sorted(ctx_raw):
                if namespace.upper() == "CLIENTCONTEXT" or namespace in resolved.clear_context_namespaces:
                    continue
                plsql_statements.append(f"DBMS_SESSION.CLEAR_CONTEXT(:p_clr_ns_{idx});")
                bind_params[f"p_clr_ns_{idx}"] = namespace
                idx += 1
    if resolved.reset_package:
        plsql_statements.append("DBMS_SESSION.RESET_PACKAGE;")

    if not plsql_statements:
        return None, bind_params, piggyback_attrs
    return f"BEGIN {' '.join(plsql_statements)} END;", bind_params, piggyback_attrs


def _wallet_pem_has_non_strict_ca(pem_path: Path) -> bool:
    """Return True when ``ewallet.pem`` contains an X.509 v1 or non-CA self-signed certificate."""
    x509_mod: Any = import_optional("cryptography.x509")
    if x509_mod is None:
        return False
    try:
        pem_bytes = pem_path.read_bytes()
        certificates = x509_mod.load_pem_x509_certificates(pem_bytes)
    except (OSError, ValueError, TypeError):
        return False
    v1_version: Any = getattr(x509_mod.Version, "v1", None)
    basic_constraints_cls: Any = x509_mod.BasicConstraints
    for cert in certificates:
        if v1_version is not None and cert.version == v1_version:
            return True
        if cert.subject == cert.issuer:
            is_ca = False
            try:
                ext = cert.extensions.get_extension_for_class(basic_constraints_cls)
                is_ca = bool(ext.value.ca)
            except Exception:
                is_ca = False
            if not is_ca:
                return True
    return False


def _cleanup_atexit_wallet_dirs() -> None:
    with _WALLET_DIR_LOCK:
        pending = list(_ATEXIT_WALLET_DIRS)
        _ATEXIT_WALLET_DIRS.clear()
        _WALLET_DIR_REFCOUNTS.clear()
    for dir_path in pending:
        shutil.rmtree(dir_path, ignore_errors=True)


def _ensure_atexit_wallet_hook_locked() -> None:
    if not _ATEXIT_WALLET_HOOK_REGISTERED[0]:
        atexit.register(_cleanup_atexit_wallet_dirs)
        _ATEXIT_WALLET_HOOK_REGISTERED[0] = True


def _track_atexit_wallet_dir(wallet_dir: Path) -> None:
    resolved = wallet_dir.resolve()
    with _WALLET_DIR_LOCK:
        _ATEXIT_WALLET_DIRS.add(resolved)
        _ensure_atexit_wallet_hook_locked()


def _default_temp_wallet_dir(prefix: str, digest: str) -> Path:
    getuid = getattr(os, "getuid", None)
    uid_part = f"{getuid()}_" if callable(getuid) else ""
    return Path(tempfile.gettempdir()) / f"{prefix}{uid_part}{digest[:16]}"


def _ensure_private_wallet_dir(dir_path: Path, *, check_owner: bool) -> None:
    if dir_path.is_symlink():
        msg = f"Unsafe symlink for Oracle wallet directory: {dir_path}"
        raise ImproperConfigurationError(msg)
    dir_path.mkdir(mode=_WALLET_DIR_MODE, parents=True, exist_ok=True)
    st = os.lstat(dir_path)
    if stat.S_ISLNK(st.st_mode) or not stat.S_ISDIR(st.st_mode):
        msg = f"Unsafe non-directory or symlink for Oracle wallet directory: {dir_path}"
        raise ImproperConfigurationError(msg)
    getuid = getattr(os, "getuid", None)
    if check_owner and callable(getuid) and st.st_uid != getuid():
        msg = f"Oracle wallet directory is not owned by the current user: {dir_path}"
        raise ImproperConfigurationError(msg)
    dir_path.chmod(_WALLET_DIR_MODE)


def _compute_wallet_digest(payload: bytes, wallet_password: "str | bytes | None") -> str:
    hasher = hashlib.sha256(payload)
    if wallet_password is not None:
        pw_bytes = wallet_password.encode("utf-8") if isinstance(wallet_password, str) else bytes(wallet_password)
        hasher.update(b"\x00")
        hasher.update(pw_bytes)
    return hasher.hexdigest()


def _is_prepared_wallet_dir(wallet_dir: Path, *, check_owner: bool = True) -> bool:
    try:
        st = os.lstat(wallet_dir)
    except OSError:
        return False
    if stat.S_ISLNK(st.st_mode) or not stat.S_ISDIR(st.st_mode):
        msg = f"Unsafe symlink for Oracle wallet directory: {wallet_dir}"
        raise ImproperConfigurationError(msg)
    getuid = getattr(os, "getuid", None)
    if check_owner and callable(getuid):
        if st.st_uid != getuid():
            msg = f"Oracle wallet directory is not owned by the current user: {wallet_dir}"
            raise ImproperConfigurationError(msg)
        if stat.S_IMODE(st.st_mode) != _WALLET_DIR_MODE:
            return False
    for fname in ("ewallet.pem", "ewallet.p12", "tnsnames.ora", "sqlnet.ora"):
        if (wallet_dir / fname).is_symlink():
            msg = f"Unsafe symlink in Oracle wallet directory: {fname!r}"
            raise ImproperConfigurationError(msg)
    if (wallet_dir / "ewallet.pem").is_file():
        return True
    if (wallet_dir / "ewallet.p12").is_file():
        return False
    return (wallet_dir / "tnsnames.ora").is_file()


def _validate_wallet_directory_contents(wallet_dir: Path) -> None:
    for fname in ("ewallet.pem", "ewallet.p12", "tnsnames.ora", "sqlnet.ora"):
        if (wallet_dir / fname).is_symlink():
            msg = f"Unsafe symlink in Oracle wallet directory: {fname!r}"
            raise ImproperConfigurationError(msg)
    if (
        not (wallet_dir / "ewallet.pem").is_file()
        and not (wallet_dir / "ewallet.p12").is_file()
        and not (wallet_dir / "tnsnames.ora").is_file()
    ):
        msg = "Oracle wallet directory must contain 'ewallet.pem', 'ewallet.p12', or 'tnsnames.ora'"
        raise ImproperConfigurationError(msg)


def _write_private_wallet_file(target_path: Path, data: bytes) -> None:
    if target_path.is_symlink():
        msg = f"Unsafe symlink for Oracle wallet file: {target_path.name!r}"
        raise ImproperConfigurationError(msg)
    flags = os.O_WRONLY | os.O_CREAT | os.O_TRUNC | getattr(os, "O_NOFOLLOW", 0)
    try:
        fd = os.open(target_path, flags, _WALLET_FILE_MODE)
    except OSError as exc:
        if target_path.is_symlink():
            msg = f"Unsafe symlink for Oracle wallet file: {target_path.name!r}"
            raise ImproperConfigurationError(msg) from exc
        raise
    try:
        fchmod = getattr(os, "fchmod", None)
        if callable(fchmod):
            fchmod(fd, _WALLET_FILE_MODE)
        os.write(fd, data)
    finally:
        os.close(fd)
    if not hasattr(os, "fchmod"):
        target_path.chmod(_WALLET_FILE_MODE)


def _convert_p12_to_pem_bytes(p12_bytes: bytes, wallet_password: "str | bytes | None") -> bytes:
    serialization_mod: Any = import_optional("cryptography.hazmat.primitives.serialization")
    pkcs12_mod: Any = import_optional("cryptography.hazmat.primitives.serialization.pkcs12")
    if serialization_mod is None or pkcs12_mod is None:
        msg = "Converting Oracle 'ewallet.p12' requires the 'cryptography' package to be installed"
        raise ImproperConfigurationError(msg)

    parsed_bundle: Any = None
    if wallet_password is None:
        try:
            parsed_bundle = pkcs12_mod.load_key_and_certificates(p12_bytes, None)
        except (ValueError, TypeError):
            try:
                parsed_bundle = pkcs12_mod.load_key_and_certificates(p12_bytes, b"")
            except (ValueError, TypeError) as exc:
                msg = "Oracle wallet contains encrypted 'ewallet.p12'; provide a valid 'wallet_password'"
                raise ImproperConfigurationError(msg) from exc
    else:
        pw_bytes = wallet_password.encode("utf-8") if isinstance(wallet_password, str) else bytes(wallet_password)
        try:
            parsed_bundle = pkcs12_mod.load_key_and_certificates(p12_bytes, pw_bytes)
        except (ValueError, TypeError) as exc:
            if pw_bytes == b"":
                try:
                    parsed_bundle = pkcs12_mod.load_key_and_certificates(p12_bytes, None)
                except (ValueError, TypeError) as inner_exc:
                    msg = "Failed to decrypt Oracle 'ewallet.p12' with the provided 'wallet_password'"
                    raise ImproperConfigurationError(msg) from inner_exc
            else:
                msg = "Failed to decrypt Oracle 'ewallet.p12' with the provided 'wallet_password'"
                raise ImproperConfigurationError(msg) from exc

    private_key, certificate, additional_certs = parsed_bundle
    if private_key is None and certificate is None:
        msg = "Oracle 'ewallet.p12' did not contain a private key or certificate"
        raise ImproperConfigurationError(msg)

    pem_chunks: list[bytes] = []
    if private_key is not None:
        pem_chunks.append(
            private_key.private_bytes(
                encoding=serialization_mod.Encoding.PEM,
                format=serialization_mod.PrivateFormat.PKCS8,
                encryption_algorithm=serialization_mod.NoEncryption(),
            )
        )
    if certificate is not None:
        pem_chunks.append(certificate.public_bytes(serialization_mod.Encoding.PEM))
    if additional_certs:
        pem_chunks.extend(ca_cert.public_bytes(serialization_mod.Encoding.PEM) for ca_cert in additional_certs)
    return b"".join(pem_chunks)


def _ensure_wallet_pem(wallet_dir: Path, *, wallet_password: "str | bytes | None") -> None:
    pem_path = wallet_dir / "ewallet.pem"
    if pem_path.is_symlink():
        msg = "Unsafe symlink in Oracle wallet directory: 'ewallet.pem'"
        raise ImproperConfigurationError(msg)
    if pem_path.is_file():
        return
    p12_path = wallet_dir / "ewallet.p12"
    if p12_path.is_symlink():
        msg = "Unsafe symlink in Oracle wallet directory: 'ewallet.p12'"
        raise ImproperConfigurationError(msg)
    if not p12_path.is_file():
        return
    pem_bytes = _convert_p12_to_pem_bytes(p12_path.read_bytes(), wallet_password)
    _write_private_wallet_file(pem_path, pem_bytes)


def _copy_wallet_directory_into(src_dir: Path, dst_root: Path) -> None:
    for item in src_dir.iterdir():
        if item.is_symlink():
            msg = f"Unsafe symlink in Oracle wallet directory: {item.name!r}"
            raise ImproperConfigurationError(msg)
        if item.is_file():
            dest_file = (dst_root / item.name).resolve()
            if dst_root not in dest_file.parents:
                msg = f"Unsafe path in Oracle wallet directory: {item.name!r}"
                raise ImproperConfigurationError(msg)
            _write_private_wallet_file(dest_file, item.read_bytes())


def _prepare_wallet_directory_source(
    src_dir: Path, *, target_dir: "Path | None", prefix: str, wallet_password: "str | bytes | None"
) -> Path:
    _validate_wallet_directory_contents(src_dir)
    if target_dir is not None:
        extract_dir = Path(target_dir)
        _ensure_private_wallet_dir(extract_dir, check_owner=False)
        _copy_wallet_directory_into(src_dir, extract_dir.resolve())
        _ensure_wallet_pem(extract_dir, wallet_password=wallet_password)
        return extract_dir

    if (src_dir / "ewallet.pem").is_file() or not (src_dir / "ewallet.p12").is_file():
        return src_dir

    p12_bytes = (src_dir / "ewallet.p12").read_bytes()
    pem_bytes = _convert_p12_to_pem_bytes(p12_bytes, wallet_password)
    try:
        _write_private_wallet_file(src_dir / "ewallet.pem", pem_bytes)
    except OSError:
        digest = _compute_wallet_digest(p12_bytes + str(src_dir).encode("utf-8"), wallet_password)
        fallback_dir = _default_temp_wallet_dir(prefix, digest)
        _ensure_private_wallet_dir(fallback_dir, check_owner=True)
        _track_atexit_wallet_dir(fallback_dir)
        _copy_wallet_directory_into(src_dir, fallback_dir.resolve())
        _write_private_wallet_file(fallback_dir / "ewallet.pem", pem_bytes)
        return fallback_dir
    return src_dir


def _extract_wallet_bytes_into(
    archive_bytes: bytes, extract_dir: Path, resolved_root: Path, *, wallet_password: "str | bytes | None"
) -> None:
    try:
        with zipfile.ZipFile(io.BytesIO(archive_bytes), "r") as archive:
            for info in archive.infolist():
                raw_name = info.filename
                if stat.S_ISLNK(info.external_attr >> 16):
                    msg = f"Unsafe symlink in Oracle wallet archive: {raw_name!r}"
                    raise ImproperConfigurationError(msg)
                member_path = Path(raw_name)
                if (
                    member_path.is_absolute()
                    or ".." in member_path.parts
                    or raw_name.startswith(("/", "\\"))
                    or ":" in raw_name
                ):
                    msg = f"Unsafe path in Oracle wallet archive: {raw_name!r}"
                    raise ImproperConfigurationError(msg)
                dest_path = (resolved_root / member_path).resolve()
                if dest_path != resolved_root and resolved_root not in dest_path.parents:
                    msg = f"Unsafe path in Oracle wallet archive: {raw_name!r}"
                    raise ImproperConfigurationError(msg)

            for info in archive.infolist():
                dest_path = resolved_root / Path(info.filename)
                if info.is_dir() or info.filename.endswith("/"):
                    _ensure_private_wallet_dir(dest_path, check_owner=False)
                    continue
                if dest_path.parent != resolved_root:
                    _ensure_private_wallet_dir(dest_path.parent, check_owner=False)
                with archive.open(info, "r") as src_file:
                    _write_private_wallet_file(dest_path, src_file.read())
    except zipfile.BadZipFile as exc:
        msg = f"Invalid Oracle wallet zip archive: {exc}"
        raise ImproperConfigurationError(msg) from exc

    _ensure_wallet_pem(extract_dir, wallet_password=wallet_password)
    if not (extract_dir / "ewallet.pem").is_file() and not (extract_dir / "tnsnames.ora").is_file():
        msg = "Oracle wallet archive must contain 'ewallet.pem', 'ewallet.p12', or 'tnsnames.ora'"
        raise ImproperConfigurationError(msg)


def _read_ora_config_text(source: "str | Path", default_filename: str) -> str:
    if isinstance(source, Path):
        with contextlib.suppress(OSError):
            candidate = source / default_filename if source.is_dir() else source
            return candidate.read_text(encoding="utf-8") if candidate.is_file() else ""
        return ""
    if "\n" not in source and "\r" not in source:
        with contextlib.suppress(OSError):
            path_obj = Path(source)
            if path_obj.is_dir():
                candidate = path_obj / default_filename
                return candidate.read_text(encoding="utf-8") if candidate.is_file() else ""
            if path_obj.is_file():
                return path_obj.read_text(encoding="utf-8")
        if "=" not in source:
            return ""
    return source


def _normalize_oracle_dbms_cloud_part(raw_part: str, *, full_identifier: str) -> str:
    cleaned = raw_part.strip()
    if not cleaned:
        msg = f"Invalid Oracle table identifier: {full_identifier!r}"
        raise SQLSpecError(msg)
    if cleaned.startswith('"'):
        if len(cleaned) < _MIN_QUOTED_IDENTIFIER_LEN or not cleaned.endswith('"'):
            msg = f"Invalid Oracle table identifier: {full_identifier!r}"
            raise SQLSpecError(msg)
        raw_inner = cleaned[1:-1]
        if '"' in raw_inner.replace('""', ""):
            msg = f"Invalid Oracle table identifier: {full_identifier!r}"
            raise SQLSpecError(msg)
        inner = raw_inner.replace('""', '"')
        if not inner:
            msg = f"Invalid Oracle table identifier: {full_identifier!r}"
            raise SQLSpecError(msg)
        if _ORACLE_UPPER_SIMPLE_IDENTIFIER_PATTERN.match(inner):
            return inner
        return quote_identifier(inner)
    if not _UNQUOTED_ORACLE_IDENTIFIER_PATTERN.match(cleaned):
        msg = f"Invalid Oracle table identifier: {full_identifier!r}"
        raise SQLSpecError(msg)
    return cleaned


def _split_oracle_dbms_cloud_table(table: str) -> "tuple[str | None, str]":
    cleaned = table.strip()
    if not cleaned:
        msg = "Table name must not be empty"
        raise SQLSpecError(msg)
    parts: list[str] = []
    buf: list[str] = []
    in_quotes = False
    idx = 0
    length = len(cleaned)
    while idx < length:
        ch = cleaned[idx]
        if ch == '"':
            if in_quotes and idx + 1 < length and cleaned[idx + 1] == '"':
                buf.append('""')
                idx += 2
                continue
            in_quotes = not in_quotes
            buf.append(ch)
            idx += 1
            continue
        if ch == "." and not in_quotes:
            parts.append("".join(buf))
            buf.clear()
            idx += 1
            continue
        buf.append(ch)
        idx += 1
    if in_quotes:
        msg = f"Invalid Oracle table identifier: {table!r}"
        raise SQLSpecError(msg)
    parts.append("".join(buf))
    if len(parts) == 1:
        return None, _normalize_oracle_dbms_cloud_part(parts[0], full_identifier=table)
    if len(parts) == _QUALIFIED_TABLE_PARTS:
        return (
            _normalize_oracle_dbms_cloud_part(parts[0], full_identifier=table),
            _normalize_oracle_dbms_cloud_part(parts[1], full_identifier=table),
        )
    msg = f"Invalid Oracle table identifier: {table!r}"
    raise SQLSpecError(msg)


def _strip_ora_comment(line: str) -> str:
    in_single = False
    in_double = False
    for idx, ch in enumerate(line):
        if ch == "'" and not in_double:
            in_single = not in_single
        elif ch == '"' and not in_single:
            in_double = not in_double
        elif ch == "#" and not in_single and not in_double:
            return line[:idx]
    return line


def _scan_balanced_ora_parens(content: str, start: int) -> int:
    depth = 0
    pos = start
    length = len(content)
    in_single = False
    in_double = False
    while pos < length:
        ch = content[pos]
        if ch == "'" and not in_double:
            in_single = not in_single
        elif ch == '"' and not in_single:
            in_double = not in_double
        elif not in_single and not in_double:
            if ch == "(":
                depth += 1
            elif ch == ")":
                depth -= 1
                if depth == 0:
                    return pos + 1
        pos += 1
    return pos


def _apply_ora_directory_resolution(config: "dict[str, Any]", *, tns_alias: Any) -> None:
    ora_dir_val = config.get("config_dir") or config.get("wallet_location")
    ora_dir = Path(str(ora_dir_val)) if ora_dir_val is not None else None

    if tns_alias is not None:
        if not isinstance(tns_alias, str) or not tns_alias.strip():
            msg = f"Invalid Oracle tns_alias: {tns_alias!r}"
            raise ImproperConfigurationError(msg)
        if ora_dir is None or not (ora_dir / "tnsnames.ora").is_file():
            msg = f"Cannot resolve Oracle tns_alias {tns_alias!r}: tnsnames.ora not found in config_dir or wallet_location"
            raise ImproperConfigurationError(msg)
        resolved_dsn = parse_tnsnames_ora(ora_dir / "tnsnames.ora").get(tns_alias.strip().lower())
        if resolved_dsn is None:
            msg = f"Oracle TNS alias {tns_alias!r} not found in tnsnames.ora"
            raise ImproperConfigurationError(msg)
        config["dsn"] = resolved_dsn
    elif ora_dir is not None and isinstance(config.get("dsn"), str):
        raw_dsn = config["dsn"].strip()
        if raw_dsn and not any(ch in raw_dsn for ch in ("(", "/", ":", "@")) and (ora_dir / "tnsnames.ora").is_file():
            resolved_dsn = parse_tnsnames_ora(ora_dir / "tnsnames.ora").get(raw_dsn.lower())
            if resolved_dsn is not None:
                config["dsn"] = resolved_dsn

    if ora_dir is not None and "ssl_server_dn_match" not in config and (ora_dir / "sqlnet.ora").is_file():
        dn_match = parse_sqlnet_ora(ora_dir / "sqlnet.ora").get("SSL_SERVER_DN_MATCH")
        if dn_match is not None:
            lowered = dn_match.strip().lower()
            if lowered in {"yes", "on", "true", "1"}:
                config["ssl_server_dn_match"] = True
            elif lowered in {"no", "off", "false", "0"}:
                config["ssl_server_dn_match"] = False


def _validate_tns_token(value: str, field_name: str) -> None:
    if any(ch in value for ch in ("(", ")", "=", "'", '"', "\r", "\n", "\x00")):
        msg = f"Unsafe TNS descriptor character in {field_name}: {value!r}"
        raise ImproperConfigurationError(msg)


def _validate_oracle_identifier(value: str, field_name: str) -> None:
    if not value or not _UNQUOTED_ORACLE_IDENTIFIER_PATTERN.fullmatch(value):
        msg = f"Invalid Oracle identifier for {field_name}: {value!r}"
        raise ImproperConfigurationError(msg)


def _validate_oracle_time_zone(value: str) -> None:
    if not value or not _ORACLE_TIME_ZONE_PATTERN.fullmatch(value) or "'" in value or ";" in value:
        msg = f"Invalid Oracle session time_zone: {value!r}"
        raise ImproperConfigurationError(msg)


def _validate_oracle_nls_parameters(nls_parameters: "Mapping[str, str]") -> None:
    for key, val in nls_parameters.items():
        if not isinstance(key, str) or not _ORACLE_NLS_KEY_PATTERN.fullmatch(key.upper()):
            msg = f"Invalid Oracle NLS parameter name: {key!r}"
            raise ImproperConfigurationError(msg)
        if not isinstance(val, str) or "'" in val or ";" in val or "--" in val or "/*" in val or "\n" in val:
            msg = f"Unsafe Oracle NLS parameter value for {key!r}: {val!r}"
            raise ImproperConfigurationError(msg)


def _validate_oracle_contexts(contexts: "Mapping[str, Mapping[str, Any]]") -> None:
    for namespace, attrs in contexts.items():
        _validate_oracle_identifier(namespace, "context namespace")
        if not isinstance(attrs, Mapping):
            msg = f"Oracle context attributes for namespace {namespace!r} must be a mapping"
            raise ImproperConfigurationError(msg)
        for attr_key in attrs:
            _validate_oracle_identifier(attr_key, "context attribute")


def _resolve_callable_or_str(value: "str | Callable[[], str | None] | None") -> str | None:
    if callable(value):
        return value()
    return value


async def read_oracle_lob_async(value: Any) -> Any:
    """Read a LOB value asynchronously from an async connection or return raw data."""
    if is_async_readable(value):
        return await value.read()
    if is_readable(value):
        return value.read()
    return value


def read_oracle_lob_sync(value: Any) -> Any:
    """Read a LOB value synchronously from a sync connection or return raw data."""
    if is_readable(value):
        return value.read()
    return value


class _OracleSyncStreamDriver(Protocol):
    def _apply_session_tracing(self, connection: Any = None) -> None: ...

    connection: "OracleSyncConnection"
    driver_features: dict[str, Any]

    def handle_database_exceptions(self) -> "SyncExceptionHandler": ...

    def _check_pending_exception(self, exc_handler: "SyncExceptionHandler") -> None: ...

    def _run_with_exception_handler(
        self, exc_handler: "SyncExceptionHandler", operation: "Callable[..., Any]", *args: Any, **kwargs: Any
    ) -> "Any | None": ...

    def _resolve_row_metadata(self, description: object) -> tuple[list[str], bool]: ...


class _OracleAsyncStreamDriver(Protocol):
    def _apply_session_tracing(self, connection: Any = None) -> None: ...

    connection: "OracleAsyncConnection"
    driver_features: dict[str, Any]

    def handle_database_exceptions(self) -> "AsyncExceptionHandler": ...

    def _check_pending_exception(self, exc_handler: "AsyncExceptionHandler") -> None: ...

    async def _run_with_exception_handler(
        self,
        exc_handler: "AsyncExceptionHandler",
        operation: "Callable[..., Awaitable[Any]]",
        *args: Any,
        **kwargs: Any,
    ) -> Any: ...

    def _resolve_row_metadata(self, description: object) -> tuple[list[str], bool]: ...


class _OracleJsonBindingState:
    """Lazily resolve whether JSON-shaped values require BLOB locators."""

    __slots__ = ("_connection", "_resolved", "_use_blob", "_version_cache")

    def __init__(self, connection: Any, version_cache: Any) -> None:
        self._connection = connection
        self._version_cache = version_cache
        self._resolved = False
        self._use_blob = False

    def uses_blob(self) -> bool:
        """Return the operation-scoped JSON storage decision."""
        if not self._resolved:
            server_major = resolve_oracle_connection_major(self._connection, self._version_cache)
            compatible_major = resolve_oracle_connection_compatible_major(self._connection, self._version_cache)
            self._use_blob = (
                isinstance(server_major, int)
                and not isinstance(server_major, bool)
                and resolve_oracle_json_storage(server_major, compatible_major=compatible_major)
                != ORACLE_JSON_STORAGE_NATIVE
            )
            self._resolved = True
        return self._use_blob


def _parse_version_tuple(version: str) -> "tuple[int, int, int]":
    parts = [int(part) for part in version.split(".") if part.isdigit()]
    while len(parts) < _VERSION_COMPONENTS:
        parts.append(0)
    return parts[0], parts[1], parts[2]


ORACLEDB_VERSION: "tuple[int, int, int]" = _parse_version_tuple(oracledb_module.__version__)


def _string_exceeds_byte_limit(value: str, limit: int) -> bool:
    """Return whether a string's UTF-8 byte length exceeds the given limit."""
    char_len = len(value)
    if char_len > limit:
        return True
    if value.isascii():
        return False
    return len(value.encode("utf-8")) > limit


def _parameter_values_need_coercion(
    values: "Iterable[Any]",
    *,
    varchar2_byte_limit: int,
    raw_byte_limit: int,
    json_binding_state: _OracleJsonBindingState,
) -> bool:
    """Return whether any value requires wrapper, LOB, or JSON rewriting."""
    for value in values:
        value_type = type(value)
        if value_type in _BIND_PASSTHROUGH_TYPES:
            continue
        if value_type is str:
            if _string_exceeds_byte_limit(value, varchar2_byte_limit):
                return True
            continue
        if value_type is bytes or value_type is bytearray:
            if len(value) > raw_byte_limit:
                return True
            continue
        if value_type is dict or value_type is list or value_type is tuple:
            if is_json_payload(value) and json_binding_state.uses_blob():
                return True
            continue
        if isinstance(value, (OracleClob, OracleBlob)):
            return True
        if isinstance(value, OracleJson):
            if json_binding_state.uses_blob():
                return True
            continue
        if isinstance(value, str):
            if _string_exceeds_byte_limit(value, varchar2_byte_limit):
                return True
            continue
        if isinstance(value, (bytes, bytearray)):
            if len(value) > raw_byte_limit:
                return True
            continue
        if is_json_payload(value) and json_binding_state.uses_blob():
            return True
    return False


def _coerce_parameters_sync(
    connection: Any,
    parameters: Any,
    *,
    clob_type: Any,
    blob_type: Any,
    varchar2_byte_limit: int,
    raw_byte_limit: int,
    json_binding_state: _OracleJsonBindingState,
) -> Any:
    """Coerce one parameter container, copying mappings and sequences only when changed."""
    if isinstance(parameters, dict):
        if not _parameter_values_need_coercion(
            parameters.values(),
            varchar2_byte_limit=varchar2_byte_limit,
            raw_byte_limit=raw_byte_limit,
            json_binding_state=json_binding_state,
        ):
            return parameters
        coerced_dict: dict[Any, Any] | None = None
        for param_name, param_value in parameters.items():
            coerced_value = _coerce_value_sync(
                connection,
                param_value,
                clob_type=clob_type,
                blob_type=blob_type,
                varchar2_byte_limit=varchar2_byte_limit,
                raw_byte_limit=raw_byte_limit,
                json_binding_state=json_binding_state,
            )
            if coerced_value is not param_value:
                if coerced_dict is None:
                    coerced_dict = dict(parameters)
                coerced_dict[param_name] = coerced_value
        return parameters if coerced_dict is None else coerced_dict
    if isinstance(parameters, (list, tuple)):
        if not _parameter_values_need_coercion(
            parameters,
            varchar2_byte_limit=varchar2_byte_limit,
            raw_byte_limit=raw_byte_limit,
            json_binding_state=json_binding_state,
        ):
            return parameters
        coerced_values: list[Any] | None = None
        for index, value in enumerate(parameters):
            coerced_value = _coerce_value_sync(
                connection,
                value,
                clob_type=clob_type,
                blob_type=blob_type,
                varchar2_byte_limit=varchar2_byte_limit,
                raw_byte_limit=raw_byte_limit,
                json_binding_state=json_binding_state,
            )
            if coerced_values is None:
                if coerced_value is value:
                    continue
                coerced_values = list(parameters[:index])
            coerced_values.append(coerced_value)
        return parameters if coerced_values is None else coerced_values
    return parameters


async def _coerce_parameters_async(
    connection: Any,
    parameters: Any,
    *,
    clob_type: Any,
    blob_type: Any,
    varchar2_byte_limit: int,
    raw_byte_limit: int,
    json_binding_state: _OracleJsonBindingState,
) -> Any:
    """Async mirror of :func:`_coerce_parameters_sync`."""
    if isinstance(parameters, dict):
        if not _parameter_values_need_coercion(
            parameters.values(),
            varchar2_byte_limit=varchar2_byte_limit,
            raw_byte_limit=raw_byte_limit,
            json_binding_state=json_binding_state,
        ):
            return parameters
        coerced_dict: dict[Any, Any] | None = None
        for param_name, param_value in parameters.items():
            coerced_value = await _coerce_value_async(
                connection,
                param_value,
                clob_type=clob_type,
                blob_type=blob_type,
                varchar2_byte_limit=varchar2_byte_limit,
                raw_byte_limit=raw_byte_limit,
                json_binding_state=json_binding_state,
            )
            if coerced_value is not param_value:
                if coerced_dict is None:
                    coerced_dict = dict(parameters)
                coerced_dict[param_name] = coerced_value
        return parameters if coerced_dict is None else coerced_dict
    if isinstance(parameters, (list, tuple)):
        if not _parameter_values_need_coercion(
            parameters,
            varchar2_byte_limit=varchar2_byte_limit,
            raw_byte_limit=raw_byte_limit,
            json_binding_state=json_binding_state,
        ):
            return parameters
        coerced_values: list[Any] | None = None
        for index, value in enumerate(parameters):
            coerced_value = await _coerce_value_async(
                connection,
                value,
                clob_type=clob_type,
                blob_type=blob_type,
                varchar2_byte_limit=varchar2_byte_limit,
                raw_byte_limit=raw_byte_limit,
                json_binding_state=json_binding_state,
            )
            if coerced_values is None:
                if coerced_value is value:
                    continue
                coerced_values = list(parameters[:index])
            coerced_values.append(coerced_value)
        return parameters if coerced_values is None else coerced_values
    return parameters


def _coerce_value_sync(
    connection: Any,
    value: Any,
    *,
    clob_type: Any,
    blob_type: Any,
    varchar2_byte_limit: int,
    raw_byte_limit: int,
    json_binding_state: _OracleJsonBindingState,
) -> Any:
    """Route a single parameter value through wrapper-aware coercion (sync)."""
    value_type = type(value)
    if value_type in _BIND_PASSTHROUGH_TYPES:
        return value
    if value_type is str:
        if _string_exceeds_byte_limit(value, varchar2_byte_limit):
            return connection.createlob(clob_type, value)
        return value
    if value_type is bytes:
        if len(value) > raw_byte_limit:
            return connection.createlob(blob_type, value)
        return value
    if value_type is bytearray:
        if len(value) > raw_byte_limit:
            return connection.createlob(blob_type, bytes(value))
        return value
    if value_type is dict or value_type is list or value_type is tuple:
        if is_json_payload(value) and json_binding_state.uses_blob():
            return connection.createlob(blob_type, to_json(value, as_bytes=True))
        return value
    if isinstance(value, OracleClob):
        inner = value.value
        if isinstance(inner, bytes):
            inner = inner.decode("utf-8")
        return connection.createlob(clob_type, inner)
    if isinstance(value, OracleBlob):
        inner = value.value
        if isinstance(inner, str):
            inner = inner.encode("utf-8")
        return connection.createlob(blob_type, inner)
    if isinstance(value, OracleJson):
        if json_binding_state.uses_blob():
            return connection.createlob(blob_type, to_json(value.value, as_bytes=True))
        return value
    if isinstance(value, str) and _string_exceeds_byte_limit(value, varchar2_byte_limit):
        return connection.createlob(clob_type, value)
    if isinstance(value, (bytes, bytearray)) and len(value) > raw_byte_limit:
        return connection.createlob(blob_type, bytes(value))
    if is_json_payload(value) and json_binding_state.uses_blob():
        return connection.createlob(blob_type, to_json(value, as_bytes=True))
    return value


async def _coerce_value_async(
    connection: Any,
    value: Any,
    *,
    clob_type: Any,
    blob_type: Any,
    varchar2_byte_limit: int,
    raw_byte_limit: int,
    json_binding_state: _OracleJsonBindingState,
) -> Any:
    """Async mirror of :func:`_coerce_value_sync`."""
    value_type = type(value)
    if value_type in _BIND_PASSTHROUGH_TYPES:
        return value
    if value_type is str:
        if _string_exceeds_byte_limit(value, varchar2_byte_limit):
            return await connection.createlob(clob_type, value)
        return value
    if value_type is bytes:
        if len(value) > raw_byte_limit:
            return await connection.createlob(blob_type, value)
        return value
    if value_type is bytearray:
        if len(value) > raw_byte_limit:
            return await connection.createlob(blob_type, bytes(value))
        return value
    if value_type is dict or value_type is list or value_type is tuple:
        if is_json_payload(value) and json_binding_state.uses_blob():
            return await connection.createlob(blob_type, to_json(value, as_bytes=True))
        return value
    if isinstance(value, OracleClob):
        inner = value.value
        if isinstance(inner, bytes):
            inner = inner.decode("utf-8")
        return await connection.createlob(clob_type, inner)
    if isinstance(value, OracleBlob):
        inner = value.value
        if isinstance(inner, str):
            inner = inner.encode("utf-8")
        return await connection.createlob(blob_type, inner)
    if isinstance(value, OracleJson):
        if json_binding_state.uses_blob():
            return await connection.createlob(blob_type, to_json(value.value, as_bytes=True))
        return value
    if isinstance(value, str) and _string_exceeds_byte_limit(value, varchar2_byte_limit):
        return await connection.createlob(clob_type, value)
    if isinstance(value, (bytes, bytearray)) and len(value) > raw_byte_limit:
        return await connection.createlob(blob_type, bytes(value))
    if is_json_payload(value) and json_binding_state.uses_blob():
        return await connection.createlob(blob_type, to_json(value, as_bytes=True))
    return value


def _description_requires_lob_coercion(description: "list[Any]") -> bool:
    """Return True when cursor metadata indicates LOB-compatible columns."""
    for column in description:
        try:
            type_code = column[1]
        except (TypeError, IndexError, KeyError):
            type_code = getattr(column, "type_code", None)
            if type_code is None:
                return True

        type_name = getattr(type_code, "name", None)
        if isinstance(type_name, str):
            upper_name = type_name.upper()
            if any(marker in upper_name for marker in _LOB_TYPE_NAME_MARKERS):
                return True
            continue

        type_text = str(type_code).upper()
        if any(marker in type_text for marker in _LOB_TYPE_NAME_MARKERS):
            return True
    return False


def _format_insert_column(column: str) -> str:
    """Format a column identifier for INSERT statements, quoting special names."""
    if _UNQUOTED_ORACLE_IDENTIFIER_PATTERN.fullmatch(column) and column.upper() not in _ORACLE_RESERVED_COLUMN_NAMES:
        return column
    escaped = column.replace('"', '""')
    return f'"{escaped}"'


def _row_requires_lob_coercion(row: "tuple[Any, ...]") -> bool:
    """Return True when a row contains readable values that need LOB coercion."""
    for value in row:
        value_type = type(value)
        if value_type in _SCALAR_PASSTHROUGH_TYPES:
            continue
        if is_readable(value) or is_async_readable(value):
            return True
    return False


def _coerce_sync_row_values(row: "tuple[Any, ...]") -> "tuple[Any, ...]":
    """Coerce LOB handles to concrete values for synchronous execution.

    Processes each value in the row, reading locator objects returned for
    OUT binds, PL/SQL, or explicit ``fetch_lobs=True``. Direct-fetched
    ``str``/``bytes`` values from the default path pass through unchanged.

    Args:
        row: Tuple of column values from database fetch.

    Returns:
        Tuple of coerced values with LOBs read to strings/bytes.
    """
    coerced_values: list[Any] | None = None
    for index, value in enumerate(row):
        value_type = type(value)
        if value_type in _SCALAR_PASSTHROUGH_TYPES:
            if coerced_values is not None:
                coerced_values.append(value)
            continue

        if is_readable(value):
            processed_value = value.read()
            if coerced_values is None:
                coerced_values = list(row[:index])
            coerced_values.append(processed_value)
            continue

        if coerced_values is not None:
            coerced_values.append(value)

    if coerced_values is None:
        return row
    return tuple(coerced_values)


async def _coerce_async_row_values(row: "tuple[Any, ...]") -> "tuple[Any, ...]":
    """Coerce LOB handles to concrete values for asynchronous execution.

    Processes each value in the row, reading locator objects returned for
    OUT binds, PL/SQL, or explicit ``fetch_lobs=True``. Direct-fetched
    ``str``/``bytes`` values from the default path pass through unchanged.

    Args:
        row: Tuple of column values from database fetch.

    Returns:
        Tuple of coerced values with LOBs read to strings/bytes.
    """
    coerced_values: list[Any] | None = None
    for index, value in enumerate(row):
        value_type = type(value)
        if value_type in _SCALAR_PASSTHROUGH_TYPES:
            if coerced_values is not None:
                coerced_values.append(value)
            continue

        if is_readable(value) or is_async_readable(value):
            processed_value = await read_oracle_lob_async(value)
            if coerced_values is None:
                if processed_value is value:
                    continue
                coerced_values = list(row[:index])
            coerced_values.append(processed_value)
            continue

        if coerced_values is not None:
            coerced_values.append(value)

    if coerced_values is None:
        return row
    return tuple(coerced_values)


def _create_oracle_error(
    error: Any, code: "int | None", error_class: type[SQLSpecError], description: str
) -> SQLSpecError:
    """Create a SQLSpec exception from an Oracle error.

    Args:
        error: The original Oracle exception
        code: Oracle error code
        error_class: The SQLSpec exception class to instantiate
        description: Human-readable description of the error type

    Returns:
        A new SQLSpec exception instance with the original as its cause
    """
    msg = f"Oracle {description} [ORA-{code:05d}]: {error}" if code else f"Oracle {description}: {error}"
    exc = error_class(msg)
    exc.__cause__ = error
    return exc
