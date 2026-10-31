"""DuckDB adapter compiled helpers."""

import contextlib
import os
import re
import tempfile
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import TYPE_CHECKING, Any, Final, cast
from urllib.parse import urlsplit

from sqlglot import Dialect, exp, parse
from sqlglot.errors import ParseError

from sqlspec.adapters.duckdb._typing import (
    DuckDBBinderException,
    DuckDBCatalogException,
    DuckDBConstraintException,
    DuckDBConversionException,
    DuckDBInterruptException,
    DuckDBIOException,
    DuckDBParserException,
    DuckDBPermissionException,
    DuckDBTransactionException,
    duckdb_module,
)
from sqlspec.core import DriverParameterProfile, ParameterStyle, StatementConfig, build_statement_config_from_profile
from sqlspec.exceptions import (
    CheckViolationError,
    DataError,
    ForeignKeyViolationError,
    ImproperConfigurationError,
    IntegrityError,
    NotFoundError,
    NotNullViolationError,
    OperationalError,
    OperationCancelledError,
    PermissionDeniedError,
    SerializationConflictError,
    SQLParsingError,
    SQLSpecError,
    UniqueViolationError,
)
from sqlspec.storage import ResolvedStorageTarget
from sqlspec.storage.backends.fsspec import FSSpecBackend
from sqlspec.storage.backends.obstore import ObStoreBackend
from sqlspec.utils.logging import get_logger
from sqlspec.utils.serializers import from_json, to_json
from sqlspec.utils.text import quote_identifier, split_qualified_identifier
from sqlspec.utils.type_converters import build_decimal_converter, build_uuid_coercions, time_iso_convert
from sqlspec.utils.type_guards import has_rowcount
from sqlspec.utils.uuids import uuid4, uuid_from_string

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping, Sequence

    from sqlspec.storage import StorageDestination, SyncStoragePipeline


__all__ = (
    "ARROW_MIN_ROWS",
    "apply_driver_features",
    "build_connection_config",
    "build_profile",
    "build_statement_config",
    "cleanup_duckdb_profile_path",
    "collect_rows",
    "configure_duckdb_statement_profiling",
    "create_duckdb_profile_path",
    "create_mapped_exception",
    "default_statement_config",
    "description_has_arrow_divergent_types",
    "driver_profile",
    "duckdb_copy_filename_follows_query",
    "duckdb_version_tuple",
    "format_identifier",
    "normalize_execute_parameters",
    "read_duckdb_profile_json",
    "resolve_duckdb_memory_limit",
    "resolve_rowcount",
    "validate_duckdb_temp_directory",
)

logger = get_logger("sqlspec.adapters.duckdb.core")

ARROW_MIN_ROWS: Final[int] = 64
_VERSION_COMPONENTS: Final[int] = 3
_CGROUP_UNBOUNDED_THRESHOLD: Final[int] = 1 << 60
_MEMORY_LIMIT_RE: Final[re.Pattern[str]] = re.compile(r"^\s*(\d+(?:\.\d+)?)\s*([A-Za-z]*)\s*$")
_MEMORY_UNIT_MULTIPLIERS: Final[dict[str, int]] = {
    "": 1,
    "B": 1,
    "KB": 1000,
    "KIB": 1024,
    "K": 1024,
    "MB": 1000**2,
    "MIB": 1024**2,
    "M": 1024**2,
    "GB": 1000**3,
    "GIB": 1024**3,
    "G": 1024**3,
    "TB": 1000**4,
    "TIB": 1024**4,
    "T": 1024**4,
}

_FIXED_ARRAY_PATTERN: Final[re.Pattern[str]] = re.compile(r"\[\d+\]")
_ARROW_SAFE_SCALAR_TYPES: Final[frozenset[str]] = frozenset({
    "INTEGER",
    "BIGINT",
    "VARCHAR",
    "DOUBLE",
    "BOOLEAN",
    "DATE",
    "TIMESTAMP",
    "SMALLINT",
    "TINYINT",
    "UBIGINT",
    "UINTEGER",
    "USMALLINT",
    "UTINYINT",
    "FLOAT",
    "BLOB",
    "UUID",
    "TIME",
    "TIMESTAMP WITH TIME ZONE",
    "TIMESTAMPTZ",
    "TIMESTAMP_S",
    "TIMESTAMP_MS",
    "JSON",
})


def description_has_arrow_divergent_types(description: "Sequence[Any] | None") -> bool:
    """Return whether any column in ``cursor.description`` diverges between Arrow and ``fetchall()``."""
    if not description:
        return False
    for column in description:
        if len(column) <= 1 or column[1] is None:
            continue
        type_str = str(column[1]).upper()
        if type_str in _ARROW_SAFE_SCALAR_TYPES:
            continue
        if _type_str_is_arrow_divergent(type_str):
            return True
    return False


def format_identifier(identifier: str) -> str:
    """Format an identifier with safe double-quote escaping.

    Args:
        identifier: Raw table or schema-qualified identifier string.

    Returns:
        Properly quoted identifier string.

    Raises:
        SQLSpecError: If the identifier is empty.
    """
    cleaned = identifier.strip()
    if not cleaned:
        msg = "Table name must not be empty"
        raise SQLSpecError(msg)
    parts = split_qualified_identifier(cleaned, quote_chars='"')
    return ".".join(quote_identifier(part) for part in parts)


_TIME_TO_ISO = time_iso_convert
_DECIMAL_TO_STRING = build_decimal_converter(mode="string")


def collect_rows(fetched_data: "list[Any] | None", description: "list[Any] | None") -> "tuple[list[Any], list[str]]":
    """Collect DuckDB rows and column names.

    Returns raw data without dict conversion. The row format is detected
    by the driver and passed to ``create_execution_result`` so that
    ``SQLResult`` can handle lazy dict materialisation.

    Args:
        fetched_data: Rows returned from cursor.fetchall().
        description: Cursor description metadata.

    Returns:
        Tuple of (rows, column_names).
    """
    if not description:
        return [], []
    column_names = [col[0] for col in description]
    if not fetched_data:
        return [], column_names
    return fetched_data, column_names


def resolve_duckdb_memory_limit(
    memory_limit: "str | None" = None,
    *,
    memory_ceiling_fraction: float = 0.8,
    cgroup_v2_path: "Path | None" = None,
    cgroup_v1_path: "Path | None" = None,
    host_memory_bytes: "int | None" = None,
) -> "str | None":
    """Resolve or clamp DuckDB ``memory_limit`` against container cgroup or host memory.

    Args:
        memory_limit: Explicit memory limit string (such as ``'4GB'`` or ``'512MB'``), or ``None``.
        memory_ceiling_fraction: Fraction of available memory used as the maximum ceiling.
        cgroup_v2_path: Optional override path for cgroup v2 ``memory.max``.
        cgroup_v1_path: Optional override path for cgroup v1 ``memory.limit_in_bytes``.
        host_memory_bytes: Optional override for host physical memory in bytes.

    Returns:
        Resolved DuckDB memory limit string, or ``memory_limit`` when system memory cannot be determined.
    """
    available_bytes = _detect_available_memory_bytes(
        cgroup_v2_path=cgroup_v2_path, cgroup_v1_path=cgroup_v1_path, host_memory_bytes=host_memory_bytes
    )
    if available_bytes is None or memory_ceiling_fraction <= 0:
        return memory_limit

    ceiling_bytes = max(1024 * 1024, int(available_bytes * memory_ceiling_fraction))
    ceiling_mb_str = f"{max(1, ceiling_bytes // (1024 * 1024))}MB"
    if memory_limit is None:
        return ceiling_mb_str

    requested_bytes = _parse_memory_size_bytes(memory_limit)
    if requested_bytes is not None and requested_bytes > ceiling_bytes:
        logger.warning(
            "Requested DuckDB memory_limit %r exceeds %.0f%% of available memory (%s); clamping to %s",
            memory_limit,
            memory_ceiling_fraction * 100,
            ceiling_mb_str,
            ceiling_mb_str,
        )
        return ceiling_mb_str
    return memory_limit


def validate_duckdb_temp_directory(
    temp_directory: "str | Path", *, reject_tmpfs: bool = False, mounts_content: "str | None" = None
) -> bool:
    """Validate whether a DuckDB spill ``temp_directory`` resides on an in-memory ``tmpfs``/``ramfs`` mount.

    Args:
        temp_directory: Target spill directory path.
        reject_tmpfs: Raise ``ImproperConfigurationError`` when the directory is backed by ``tmpfs`` or ``ramfs``.
        mounts_content: Optional ``/proc/mounts`` text override for testing.

    Returns:
        ``True`` when the spill directory is not on ``tmpfs``/``ramfs``, or ``False`` when a warning was emitted.

    Raises:
        ImproperConfigurationError: If ``reject_tmpfs=True`` and the path resides on ``tmpfs`` or ``ramfs``.
    """
    raw_mounts = mounts_content
    if raw_mounts is None:
        mounts_path = Path("/proc/mounts")
        if mounts_path.exists():
            with contextlib.suppress(OSError):
                raw_mounts = mounts_path.read_text(encoding="utf-8")
    if not raw_mounts:
        return True

    target_path = Path(temp_directory)
    with contextlib.suppress(OSError):
        if target_path.exists():
            target_path = target_path.resolve()
        elif target_path.parent.exists():
            target_path = target_path.parent.resolve() / target_path.name
    target_str = str(target_path)

    best_mount_point = ""
    best_fs_type = ""
    for line in raw_mounts.splitlines():
        with contextlib.suppress(ValueError):
            _device, mount_point, raw_fs_type, *_rest = line.split()
            fs_type = raw_fs_type.lower()
            normalized_mount = mount_point.rstrip("/") or "/"
            matches = target_str == normalized_mount or target_str.startswith(
                "/" if normalized_mount == "/" else f"{normalized_mount}/"
            )
            if matches and len(normalized_mount) >= len(best_mount_point):
                best_mount_point = normalized_mount
                best_fs_type = fs_type

    if best_fs_type in {"tmpfs", "ramfs"}:
        msg = (
            f"DuckDB temp_directory {str(temp_directory)!r} resides on in-memory filesystem "
            f"{best_fs_type!r} (mount {best_mount_point!r}), which risks container OOM during out-of-core spills."
        )
        if reject_tmpfs:
            raise ImproperConfigurationError(msg)
        logger.warning(msg)
        return False
    return True


def build_connection_config(connection_config: "Mapping[str, Any]") -> "dict[str, Any]":
    """Build connection configuration for pool creation.

    Args:
        connection_config: Raw connection configuration mapping.

    Returns:
        Dictionary with connection parameters.
    """
    pool_only_keys = {
        "pool_min_size",
        "pool_max_size",
        "pool_timeout",
        "pool_recycle_seconds",
        "health_check_interval",
        "memory_ceiling_fraction",
        "reject_tmpfs_temp_directory",
    }
    alias_keys = {"db", "path", "file"}
    connect_parameters: dict[str, Any] = {}
    duckdb_config: dict[str, Any] = {}

    nested_config = connection_config.get("config")
    if isinstance(nested_config, dict):
        duckdb_config.update({key: value for key, value in nested_config.items() if value is not None})

    database = (
        connection_config.get("database")
        or connection_config.get("db")
        or connection_config.get("path")
        or connection_config.get("file")
    )
    if database is not None:
        connect_parameters["database"] = database

    for key, value in connection_config.items():
        if value is None or key in pool_only_keys or key in alias_keys or key in {"database", "config", "extra"}:
            continue
        if key == "read_only":
            connect_parameters[key] = value
        else:
            duckdb_config[key] = value

    extra = connection_config.get("extra")
    if isinstance(extra, dict):
        duckdb_config.update({key: value for key, value in extra.items() if value is not None})

    if duckdb_config.get("temp_directory"):
        validate_duckdb_temp_directory(
            duckdb_config["temp_directory"],
            reject_tmpfs=bool(connection_config.get("reject_tmpfs_temp_directory", False)),
        )

    if "memory_ceiling_fraction" in connection_config or "memory_limit" in duckdb_config:
        fraction = float(connection_config.get("memory_ceiling_fraction", 0.8))
        resolved_limit = resolve_duckdb_memory_limit(
            cast("str | None", duckdb_config.get("memory_limit")), memory_ceiling_fraction=fraction
        )
        if resolved_limit is not None:
            duckdb_config["memory_limit"] = resolved_limit

    if duckdb_config:
        connect_parameters["config"] = duckdb_config

    return connect_parameters


def normalize_execute_parameters(parameters: Any) -> Any:
    """Normalize parameters for DuckDB execute calls.

    Args:
        parameters: Prepared parameters payload.

    Returns:
        Normalized parameters payload.
    """
    return parameters or ()


def resolve_rowcount(cursor: Any) -> int:
    """Resolve rowcount from DuckDB cursor results.

    Args:
        cursor: DuckDB cursor object.

    Returns:
        Rowcount value derived from cursor output.
    """
    try:
        result = cursor.fetchone()
    except Exception:
        result = None
    if result and isinstance(result, tuple) and len(result) == 1:
        try:
            return int(result[0])
        except (TypeError, ValueError):
            result = None
    if has_rowcount(cursor):
        return max(cursor.rowcount, 0)
    return 0


def build_profile() -> "DriverParameterProfile":
    """Create the DuckDB driver parameter profile."""

    return DriverParameterProfile(
        name="DuckDB",
        default_style=ParameterStyle.QMARK,
        supported_styles={ParameterStyle.QMARK, ParameterStyle.NUMERIC, ParameterStyle.NAMED_DOLLAR},
        default_execution_style=ParameterStyle.QMARK,
        supported_execution_styles={ParameterStyle.QMARK, ParameterStyle.NUMERIC},
        has_native_list_expansion=True,
        preserve_parameter_format=True,
        needs_static_script_compilation=False,
        allow_mixed_parameter_styles=False,
        preserve_original_params_for_many=False,
        json_serializer_strategy="helper",
        custom_type_coercions={
            bool: _identity,
            datetime: _TIME_TO_ISO,
            date: _TIME_TO_ISO,
            Decimal: _DECIMAL_TO_STRING,
            **build_uuid_coercions(),
        },
        default_dialect="duckdb",
    )


def apply_driver_features(
    statement_config: "StatementConfig", driver_features: "Mapping[str, Any] | None"
) -> "tuple[StatementConfig, dict[str, Any]]":
    """Apply DuckDB-specific driver features to statement configuration."""
    features: dict[str, Any] = dict(driver_features) if driver_features else {}
    features.setdefault("enable_events", True)
    features.setdefault("events_backend", "poll_queue")
    features.setdefault("enable_statement_profiling", False)

    param_config = statement_config.parameter_config
    json_serializer = features.get("json_serializer")
    if json_serializer:
        param_config = param_config.with_json_serializers(
            cast("Callable[[Any], str]", json_serializer), tuple_strategy="tuple"
        )

    if not features.get("enable_uuid_conversion", True):
        type_coercion_map = dict(param_config.type_coercion_map)
        for uuid_type in build_uuid_coercions():
            type_coercion_map.pop(uuid_type, None)
        param_config = param_config.replace(type_coercion_map=type_coercion_map)

    if param_config is statement_config.parameter_config:
        return statement_config, features
    return statement_config.replace(parameter_config=param_config), features


def create_duckdb_profile_path() -> Path:
    """Allocate a unique temporary file path for DuckDB JSON statement profiling."""
    return Path(tempfile.gettempdir()) / f"sqlspec_duckdb_profile_{uuid4().hex}.json"


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


def configure_duckdb_statement_profiling(connection: Any, profile_path: Path) -> None:
    """Enable DuckDB JSON statement profiling on a connection without observer emission."""
    escaped_path = str(profile_path).replace("'", "''")
    connection.execute("PRAGMA enable_profiling='json'")
    with contextlib.suppress(Exception):
        connection.execute("SET profiling_coverage='ALL'")
    connection.execute(f"SET profiling_output='{escaped_path}'")


_EXCEPTION_MAPPING: Final[dict[type[BaseException], tuple[type[SQLSpecError], str]]] = {
    DuckDBCatalogException: (NotFoundError, "catalog error"),
    DuckDBParserException: (SQLParsingError, "SQL parsing error"),
    DuckDBBinderException: (SQLParsingError, "SQL parsing error"),
    DuckDBPermissionException: (PermissionDeniedError, "permission denied"),
    DuckDBInterruptException: (OperationCancelledError, "query interrupted"),
    DuckDBIOException: (OperationalError, "operational error"),
    DuckDBConversionException: (DataError, "data error"),
}
_EXCEPTION_MAPPING_CACHE: Final[dict[type[BaseException], tuple[type[SQLSpecError], str]]] = {}


def create_mapped_exception(error: "BaseException", *, logger: Any | None = None) -> SQLSpecError:
    """Map DuckDB exceptions to SQLSpec exceptions.

    This is a factory function that returns an exception instance rather than
    raising. This pattern is more robust for use in __exit__ handlers and
    avoids issues with exception control flow in different Python versions.

    Mapping priority:
        1. ConstraintException -> message-pattern sub-classification (Unique/FK/NotNull/Check)
        2. Native DuckDB exception type via dispatch table (MRO-walked, cached)
        3. Type-name substring fallback for exception-like inputs
        4. Message-pattern fallback for unrelated types (permission/interrupt/type-mismatch)
        5. Default SQLSpecError fallback

    Args:
        error: The DuckDB exception to map
        logger: Optional logger accepted for adapter signature parity.

    Returns:
        A SQLSpec exception that wraps the original error
    """
    del logger
    exc_type = type(error)
    if isinstance(error, DuckDBConstraintException):
        return _classify_duckdb_constraint(error)
    if isinstance(error, DuckDBTransactionException):
        return _map_transaction_exception(error)

    mapped = _resolve_duckdb_exception_mapping(exc_type)
    if mapped is not None:
        error_class, description = mapped
        return _create_duckdb_error(error, error_class, description)

    exc_name = exc_type.__name__.lower()
    if "constraintexception" in exc_name:
        return _classify_duckdb_constraint(error)
    if "transactionexception" in exc_name:
        return _map_transaction_exception(error)
    if "catalogexception" in exc_name:
        return _create_duckdb_error(error, NotFoundError, "catalog error")
    if "parserexception" in exc_name or "binderexception" in exc_name:
        return _create_duckdb_error(error, SQLParsingError, "SQL parsing error")
    if "permissionexception" in exc_name:
        return _create_duckdb_error(error, PermissionDeniedError, "permission denied")
    if "interruptexception" in exc_name:
        return _create_duckdb_error(error, OperationCancelledError, "query interrupted")
    if "ioexception" in exc_name:
        return _create_duckdb_error(error, OperationalError, "operational error")
    if "conversionexception" in exc_name:
        return _create_duckdb_error(error, DataError, "data error")

    error_msg = str(error).lower()
    if "permission denied" in error_msg or "access denied" in error_msg:
        return _create_duckdb_error(error, PermissionDeniedError, "permission denied")
    if "interrupt" in error_msg or "cancel" in error_msg:
        return _create_duckdb_error(error, OperationCancelledError, "query canceled")
    if "type mismatch" in error_msg:
        return _create_duckdb_error(error, DataError, "data error")
    if "conflict on update" in error_msg:
        return _create_duckdb_error(error, SerializationConflictError, "serialization conflict")

    return _create_duckdb_error(error, SQLSpecError, "database error")


def build_statement_config(*, json_serializer: "Callable[[Any], str] | None" = None) -> StatementConfig:
    """Construct the DuckDB statement configuration with optional JSON serializer."""
    serializer = json_serializer or to_json
    profile = driver_profile
    return build_statement_config_from_profile(
        profile, statement_overrides={"dialect": "duckdb"}, json_serializer=serializer
    )


def duckdb_version_tuple() -> "tuple[int, int, int]":
    """Return the installed DuckDB version as a 3-tuple of integers."""
    try:
        raw = getattr(duckdb_module, "__version__", "")
        parts = [int(part) for part in raw.split(".") if part.isdigit()]
        while len(parts) < _VERSION_COMPONENTS:
            parts.append(0)
        return parts[0], parts[1], parts[2]
    except Exception:
        return (0, 0, 0)


_DUCKDB_COPY_FILENAME_FOLLOWS_QUERY: Final[bool] = duckdb_version_tuple() >= (2, 0, 0)


def duckdb_copy_filename_follows_query() -> bool:
    """Return whether DuckDB COPY TO parameter binding follows query bindings."""
    return _DUCKDB_COPY_FILENAME_FOLLOWS_QUERY


def _parse_memory_size_bytes(memory_limit: str) -> "int | None":
    match = _MEMORY_LIMIT_RE.match(memory_limit)
    if match is None:
        return None
    amount = float(match.group(1))
    unit = match.group(2).upper()
    multiplier = _MEMORY_UNIT_MULTIPLIERS.get(unit)
    if multiplier is None:
        return None
    return int(amount * multiplier)


def _detect_available_memory_bytes(
    *,
    cgroup_v2_path: "Path | None" = None,
    cgroup_v1_path: "Path | None" = None,
    host_memory_bytes: "int | None" = None,
) -> "int | None":
    v2_file = cgroup_v2_path if cgroup_v2_path is not None else Path("/sys/fs/cgroup/memory.max")
    if v2_file.exists():
        with contextlib.suppress(OSError, ValueError):
            raw_v2 = v2_file.read_text(encoding="utf-8").strip()
            if raw_v2 and raw_v2.lower() != "max":
                parsed_v2 = int(raw_v2)
                if 0 < parsed_v2 < _CGROUP_UNBOUNDED_THRESHOLD:
                    return parsed_v2

    v1_file = cgroup_v1_path if cgroup_v1_path is not None else Path("/sys/fs/cgroup/memory/memory.limit_in_bytes")
    if v1_file.exists():
        with contextlib.suppress(OSError, ValueError):
            raw_v1 = v1_file.read_text(encoding="utf-8").strip()
            if raw_v1:
                parsed_v1 = int(raw_v1)
                if 0 < parsed_v1 < _CGROUP_UNBOUNDED_THRESHOLD:
                    return parsed_v1

    if host_memory_bytes is not None and host_memory_bytes > 0:
        return host_memory_bytes

    sysconf = getattr(os, "sysconf", None)
    if sysconf is not None:
        with contextlib.suppress(ValueError, OSError, AttributeError):
            page_size = sysconf("SC_PAGE_SIZE")
            phys_pages = sysconf("SC_PHYS_PAGES")
            if isinstance(page_size, int) and isinstance(phys_pages, int) and page_size > 0 and phys_pages > 0:
                return page_size * phys_pages
    return None


def _build_storage_copy_sql(sql: str, file_format: str) -> str | None:
    """Wrap one compiled query in COPY with a separately bound filename."""
    if file_format not in {"parquet", "csv"}:
        return None
    try:
        statements = parse(sql, read="duckdb")
    except ParseError:
        return None
    if len(statements) != 1 or not isinstance(statements[0], exp.Query):
        return None
    options = [exp.CopyParameter(this=exp.Var(this="FORMAT"), expression=exp.Var(this=file_format.upper()))]
    if file_format == "csv":
        options.append(exp.CopyParameter(this=exp.Var(this="HEADER"), expression=exp.Boolean(this=True)))
    return exp.Copy(this=exp.Subquery(this=statements[0]), kind=False, files=[exp.Placeholder()], params=options).sql(
        dialect="duckdb", copy=False
    )


def _build_storage_read_sql(table: str, uri: str, file_format: str, *, allow_globs: bool = False) -> str | None:
    """Build a single-object or glob reader with a validated, quoted target identifier."""
    normalized_format = file_format.lower()
    allowed_formats = {"parquet", "csv", "jsonl", "ndjson"} if allow_globs else {"parquet"}
    if normalized_format not in allowed_formats:
        return None
    if not allow_globs and any(char in uri for char in "*?[]"):
        return None
    try:
        targets = Dialect.get_or_raise("duckdb").parse_into(exp.Table, table)
    except ParseError as exc:
        msg = "Native storage import requires one qualified table identifier."
        raise ValueError(msg) from exc
    if (
        len(targets) != 1
        or not isinstance(targets[0], exp.Table)
        or any(key not in {"this", "db", "catalog"} and value for key, value in targets[0].args.items())
        or any(not isinstance(part, exp.Identifier) for part in targets[0].parts)
    ):
        msg = "Native storage import requires one qualified table identifier."
        raise ValueError(msg)
    target = targets[0]
    for part in target.parts:
        part.set("quoted", True)
    if normalized_format == "parquet":
        reader = exp.Anonymous(
            this="read_parquet",
            expressions=[
                exp.Placeholder(),
                exp.Kwarg(this=exp.Var(this="hive_partitioning"), expression=exp.Boolean(this=False)),
            ],
        )
    elif normalized_format == "csv":
        reader = exp.Anonymous(
            this="read_csv",
            expressions=[
                exp.Placeholder(),
                exp.Kwarg(this=exp.Var(this="auto_detect"), expression=exp.Boolean(this=True)),
                exp.Kwarg(this=exp.Var(this="header"), expression=exp.Boolean(this=True)),
            ],
        )
    else:
        reader = exp.Anonymous(
            this="read_json",
            expressions=[
                exp.Placeholder(),
                exp.Kwarg(this=exp.Var(this="format"), expression=exp.Literal.string("newline_delimited")),
                exp.Kwarg(this=exp.Var(this="auto_detect"), expression=exp.Boolean(this=True)),
            ],
        )
    return exp.Insert(this=target, expression=exp.select("*").from_(reader, copy=False)).sql(
        dialect="duckdb", copy=False
    )


def _resolve_native_storage_target(
    pipeline: "SyncStoragePipeline",
    destination: "StorageDestination",
    driver_features: "Mapping[str, Any]",
    *,
    write: bool,
) -> "ResolvedStorageTarget | None":
    """Resolve explicit provider aliases without changing the native address.

    Azure destinations resolve with the account name taken from the successful
    native secret, and that account name is not treated as a backend option
    because the provisioned secret owns the credentials.
    """
    protocols = driver_features.get("_duckdb_storage_protocols", ())
    if not driver_features.get("_duckdb_storage_extensions") and not protocols:
        return None
    original = str(destination)
    if "?" in original or "#" in original:
        return None
    scheme, separator, suffix = original.partition("://")
    if separator and scheme != "alias" and scheme in protocols:
        return ResolvedStorageTarget(original, scheme)
    if not separator or scheme not in {"alias", "s3", "gs", "gcs", "r2", "az", "azure", "abfss", "http", "https"}:
        return None
    normalized = {"gcs": "gs", "r2": "s3"}.get(scheme)
    resolved_input = f"{normalized}://{suffix}" if normalized else original
    resolver_options: dict[str, Any] = {}
    if scheme in {"az", "azure", "abfss"}:
        for secret in driver_features.get("_duckdb_storage_secrets", ()):
            if secret.get("secret_type", "").lower() != "azure" or (
                secret.get("scope") and not original.startswith(secret["scope"])
            ):
                continue
            values = {key.lower(): value for key, value in secret.get("value", {}).items()}
            account = values.get("account_name")
            if not account:
                connection_parts = dict(
                    item.split("=", 1) for item in str(values.get("connection_string", "")).split(";") if "=" in item
                )
                account = connection_parts.get("AccountName")
            if account:
                resolver_options["account_name"] = account
                break
        if not resolver_options:
            return None
    target = pipeline.resolve_destination(resolved_input, storage_options=resolver_options or None)
    backend_key = suffix.partition("/")[0].strip() if scheme == "alias" else resolved_input
    backend = pipeline.registry.get(backend_key, **resolver_options)
    options: Mapping[str, Any] | None = None
    if type(backend) is ObStoreBackend:
        options = backend.store_options
    elif type(backend) is FSSpecBackend:
        options = backend.fs.storage_options
    if resolver_options:
        options = {}
    if normalized:
        target = ResolvedStorageTarget(original, scheme)
    if "?" in target.uri or "#" in target.uri:
        return None
    if target.protocol in protocols and options is not None and not options:
        return target
    if not _native_storage_eligible(target.uri, target.protocol, options, driver_features, write=write):
        return None
    return target


def _native_storage_eligible(
    uri: str,
    protocol: str,
    backend_options: "Mapping[str, Any] | None",
    driver_features: "Mapping[str, Any]",
    *,
    write: bool,
) -> bool:
    """Require completed provisioning and equivalent, understood backend settings.

    Unknown credentials or options keep the existing storage path. This check
    performs no SQL or I/O and never classifies an execution error for retry.
    """
    provider_type = {
        "s3": "s3",
        "gs": "gcs",
        "gcs": "gcs",
        "r2": "r2",
        "az": "azure",
        "azure": "azure",
        "abfss": "azure",
    }.get(protocol)
    extension = "azure" if provider_type == "azure" else "httpfs"
    if backend_options is None or extension not in driver_features.get("_duckdb_storage_extensions", ()):
        return False
    if protocol in {"http", "https"}:
        return not write and not backend_options
    if provider_type is None:
        return False

    secrets = driver_features.get("_duckdb_storage_secrets", ())
    matching = [
        secret
        for secret in secrets
        if secret.get("secret_type", "").lower() == provider_type
        and (not secret.get("scope") or uri.startswith(secret["scope"]))
    ]
    if not matching:
        return False
    matching.sort(key=lambda secret: len(secret.get("scope") or ""), reverse=True)
    selected = matching[0]
    if len(matching) > 1 and len(selected.get("scope") or "") == len(matching[1].get("scope") or ""):
        return False
    provider = selected.get("provider", "config").lower()
    native = {key.lower(): value for key, value in selected.get("value", {}).items()}
    if not backend_options:
        return provider in {"config", "credential_chain", "managed_identity", "service_principal"}
    if provider != "config":
        return False
    if provider_type == "azure":
        return _azure_storage_options_match(backend_options, native)
    return _s3_storage_options_match(backend_options, native, provider_type)


def _s3_storage_options_match(
    backend_options: "Mapping[str, Any]", native: "Mapping[str, Any]", provider_type: str
) -> bool:
    """Compare static credentials, OAuth bearer tokens, and endpoint options for S3/GCS/R2 providers."""
    if native.keys() - {
        "key_id",
        "secret",
        "session_token",
        "bearer_token",
        "token",
        "region",
        "endpoint",
        "url_style",
        "use_ssl",
        "account_id",
    }:
        return False

    options: dict[str, Any] = {}
    for key, value in backend_options.items():
        normalized = key.removeprefix("aws_").removeprefix("google_")
        if normalized in options and options[normalized] != value:
            return False
        options[normalized] = value
    client = options.pop("client_options", {})
    if not isinstance(client, dict) or client.keys() - {"allow_http"}:
        return False
    if "allow_http" in client:
        if "allow_http" in options and options["allow_http"] != client["allow_http"]:
            return False
        options["allow_http"] = client["allow_http"]
    if provider_type == "gcs" and ("bearer_token" in native or "token" in native):
        if options.keys() - {"bearer_token", "token"}:
            return False
        native_token = native.get("bearer_token") or native.get("token")
        option_token = options.get("bearer_token") or options.get("token")
        return bool(native_token and option_token == native_token)
    if options.keys() - {
        "access_key_id",
        "secret_access_key",
        "session_token",
        "region",
        "endpoint",
        "virtual_hosted_style_request",
        "allow_http",
    }:
        return False
    if not options.get("access_key_id") or not options.get("secret_access_key"):
        return False
    if any(
        options.get(source) != native.get(target)
        for source, target in (
            ("access_key_id", "key_id"),
            ("secret_access_key", "secret"),
            ("session_token", "session_token"),
        )
    ):
        return False
    if options.get("region", "us-east-1") != native.get("region", "us-east-1"):
        return False
    style = "vhost" if options.get("virtual_hosted_style_request", False) else "path"
    if native.get("url_style", "vhost" if provider_type == "s3" else "path") != style:
        return False
    native_endpoint = native.get("endpoint")
    if native_endpoint is None and provider_type == "gcs":
        native_endpoint = "storage.googleapis.com"
    elif native_endpoint is None and provider_type == "r2" and native.get("account_id"):
        native_endpoint = f"{native['account_id']}.r2.cloudflarestorage.com"
    endpoint = options.get("endpoint")
    if endpoint is None:
        return not native_endpoint and native.get("use_ssl", True) is True
    if not isinstance(endpoint, str) or not endpoint:
        return False
    parsed = urlsplit(endpoint)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc or parsed.path not in {"", "/"}:
        return False
    if parsed.query or parsed.fragment or parsed.username or parsed.password:
        return False
    if native_endpoint != parsed.netloc or native.get("use_ssl", True) != (parsed.scheme == "https"):
        return False
    return not (parsed.scheme == "http" and options.get("allow_http") is not True)


def _azure_storage_options_match(options: "Mapping[str, Any]", native: "Mapping[str, Any]") -> bool:
    """Recognize matching standard Azure account keys or a connection string."""
    if set(options) == {"connection_string"}:
        return bool(options["connection_string"] == native.get("connection_string"))
    normalized: dict[str, Any] = {}
    for key, value in options.items():
        key = key.removeprefix("azure_storage_").removeprefix("azure_")
        if key in normalized and normalized[key] != value:
            return False
        normalized[key] = value
    if normalized.keys() - {"account_name", "access_key"}:
        return False
    connection_string = native.get("connection_string")
    if not isinstance(connection_string, str) or native.keys() - {"connection_string", "account_name"}:
        return False
    parts: dict[str, str] = {}
    for item in connection_string.split(";"):
        if not item:
            continue
        key, separator, value = item.partition("=")
        if not separator or key in parts:
            return False
        parts[key] = value
    if parts.keys() - {"DefaultEndpointsProtocol", "AccountName", "AccountKey", "EndpointSuffix"}:
        return False
    return (
        parts.get("DefaultEndpointsProtocol", "https") == "https"
        and parts.get("EndpointSuffix", "core.windows.net") == "core.windows.net"
        and bool(normalized.get("account_name"))
        and bool(normalized.get("access_key"))
        and normalized.get("account_name") == parts.get("AccountName")
        and normalized.get("access_key") == parts.get("AccountKey")
        and native.get("account_name", parts.get("AccountName")) == parts.get("AccountName")
    )


def _create_duckdb_error(error: Any, error_class: type[SQLSpecError], description: str) -> SQLSpecError:
    """Create a SQLSpec exception from a DuckDB error.

    Args:
        error: The original DuckDB exception
        error_class: The SQLSpec exception class to instantiate
        description: Human-readable description of the error type

    Returns:
        A new SQLSpec exception instance with the original as its cause
    """
    msg = f"DuckDB {description}: {error}"
    exc = error_class(msg)
    exc.__cause__ = error
    return exc


def _resolve_duckdb_exception_mapping(error_type: "type[BaseException]") -> "tuple[type[SQLSpecError], str] | None":
    cached = _EXCEPTION_MAPPING_CACHE.get(error_type)
    if cached is not None:
        return cached
    direct = _EXCEPTION_MAPPING.get(error_type)
    if direct is not None:
        _EXCEPTION_MAPPING_CACHE[error_type] = direct
        return direct
    for base in error_type.__mro__[1:]:
        mapped = _EXCEPTION_MAPPING.get(base)
        if mapped is not None:
            _EXCEPTION_MAPPING_CACHE[error_type] = mapped
            return mapped
    return None


def _classify_duckdb_constraint(error: "BaseException") -> SQLSpecError:
    error_msg = str(error).lower()
    if "unique" in error_msg or "duplicate" in error_msg:
        return _create_duckdb_error(error, UniqueViolationError, "unique constraint violation")
    if "foreign key" in error_msg or "violates foreign key" in error_msg:
        return _create_duckdb_error(error, ForeignKeyViolationError, "foreign key constraint violation")
    if "not null" in error_msg or "null value" in error_msg:
        return _create_duckdb_error(error, NotNullViolationError, "not-null constraint violation")
    if "check constraint" in error_msg or "check condition" in error_msg:
        return _create_duckdb_error(error, CheckViolationError, "check constraint violation")
    return _create_duckdb_error(error, IntegrityError, "integrity constraint violation")


def _map_transaction_exception(error: "BaseException") -> SQLSpecError:
    if "conflict on update" in str(error).lower():
        return _create_duckdb_error(error, SerializationConflictError, "serialization conflict")
    return _create_duckdb_error(error, OperationalError, "transaction error")


def _identity(value: Any) -> Any:
    return value


class _DuckDBStreamSource:
    """Native DuckDB chunk source backed by an Arrow record batch reader."""

    __slots__ = ("_chunk_size", "_description", "_open_reader", "_parameters", "_reader", "_sql")

    def __init__(
        self,
        open_reader: "Callable[[str, Any, int], tuple[Any, list[Any] | None]]",
        sql: str,
        parameters: Any,
        chunk_size: int,
    ) -> None:
        self._chunk_size = chunk_size
        self._description: list[Any] | None = None
        self._open_reader = open_reader
        self._parameters = parameters
        self._reader: Any | None = None
        self._sql = sql

    def start(self) -> None:
        self._reader, self._description = self._open_reader(self._sql, self._parameters, self._chunk_size)

    def fetch_chunk(self) -> "list[dict[str, Any]]":
        reader = self._reader
        if reader is None:
            return []
        try:
            batch = reader.read_next_batch()
        except StopIteration:
            return []
        rows = cast("list[dict[str, Any]]", batch.to_pylist())
        _restore_uuid_columns(rows, self._description)
        return rows

    def close(self, error: bool = False) -> None:
        reader = self._reader
        self._reader = None
        if reader is not None:
            with contextlib.suppress(Exception):
                reader.close()


def _restore_uuid_columns(rows: "list[dict[str, Any]]", description: "list[Any] | None") -> None:
    """Restore DuckDB UUID columns after Arrow materialization."""
    if not rows or not description:
        return
    uuid_columns = [column[0] for column in description if len(column) > 1 and str(column[1]).upper() == "UUID"]
    if not uuid_columns:
        return
    for row in rows:
        for column in uuid_columns:
            value = row.get(column)
            if isinstance(value, str):
                row[column] = uuid_from_string(value)


def _type_str_is_arrow_divergent(type_str: str) -> bool:
    if (
        "HUGEINT" in type_str
        or "TIMETZ" in type_str
        or "TIME WITH TIME ZONE" in type_str
        or "TIMESTAMP_NS" in type_str
        or "INTERVAL" in type_str
        or "MAP(" in type_str
        or "BIGNUM" in type_str
        or "VARINT" in type_str
    ):
        return True
    if type_str == "BIT" or "BIT[" in type_str or " BIT" in type_str or type_str.startswith("BIT "):
        return True
    if "UUID" in type_str and type_str != "UUID":
        return True
    return "[" in type_str and _FIXED_ARRAY_PATTERN.search(type_str) is not None


driver_profile = build_profile()

default_statement_config = build_statement_config()
