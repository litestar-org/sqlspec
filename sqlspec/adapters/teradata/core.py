"""Compiled Teradata statement, connection, and error helpers."""

import re
from collections.abc import Callable, Mapping, Sequence, Sized
from logging import Logger
from typing import Any, Literal

from sqlspec.core import DriverParameterProfile, ParameterStyle, StatementConfig, build_statement_config_from_profile
from sqlspec.exceptions import (
    CheckViolationError,
    DatabaseConnectionError,
    DataError,
    DeadlockError,
    ForeignKeyViolationError,
    ImproperConfigurationError,
    NotFoundError,
    NotNullViolationError,
    OperationalError,
    PermissionDeniedError,
    QueryTimeoutError,
    SQLParsingError,
    SQLSpecError,
    UniqueViolationError,
)
from sqlspec.utils.serializers import from_json, to_json
from sqlspec.utils.text import split_qualified_identifier
from sqlspec.utils.type_converters import build_uuid_coercions

__all__ = (
    "DEFAULT_TERADATA_PORT",
    "SCHEMA_EXISTS_SQL",
    "TABLE_EXISTS_SQL",
    "apply_driver_features",
    "build_connection_config",
    "build_insert_statement",
    "build_profile",
    "build_set_query_band_sql",
    "build_statement_config",
    "collect_rows",
    "create_mapped_exception",
    "default_statement_config",
    "driver_profile",
    "extract_error_code",
    "format_identifier",
    "format_query_band",
    "format_table_ref",
    "is_ddl_or_session_band_sql",
    "maybe_prefix_fastexport",
    "render_query_band_sql",
    "resolve_column_names",
    "resolve_many_rowcount",
    "resolve_rowcount",
    "split_schema_and_table",
)


_MAX_PORT = 65535
_IDENTIFIER_PARTS = 2
_COLUMN_CACHE_SIZE = 256
_LEADING_COMMENT_PATTERN = re.compile(r"\A\s*(?:(?:/\*.*?\*/|--[^\n]*(?:\n|$))\s*)*", re.DOTALL)
_DDL_PREFIX_PATTERN = re.compile(
    r"(?:CREATE|ALTER|DROP|RENAME|REPLACE|DATABASE|MODIFY|GIVE|GRANT|REVOKE)\b|COMMENT\s+ON\b|COLLECT\s+STATISTICS\b"
)
_QUERY_BAND_KEY_PATTERN = re.compile(r"[A-Za-z0-9_.-]+")
_ERROR_CODE_PATTERN = re.compile(r"\[Error\s+(\d+)\]", re.IGNORECASE)
_ERROR_CODE_MAPPING: dict[int, type[SQLSpecError]] = {
    **dict.fromkeys((2801, 2802, 2803), UniqueViolationError),
    **dict.fromkeys((2700, 2701, 5312, 5313), ForeignKeyViolationError),
    **dict.fromkeys((3604, 3811), NotNullViolationError),
    5317: CheckViolationError,
    2631: DeadlockError,
    **dict.fromkeys((3110, 3156), QueryTimeoutError),
    **dict.fromkeys((3523, 3524, 8017), PermissionDeniedError),
    **dict.fromkeys((3706, 3707), SQLParsingError),
    **dict.fromkeys((3802, 3807, 5628), NotFoundError),
    **dict.fromkeys((2616, 2620, 2621, 2665, 6706, 6758, 6760), DataError),
    **dict.fromkeys((2644, 2646, 2843, 3932), OperationalError),
}
_CONNECTION_ERROR_MARKERS = (
    "unable to connect",
    "connection refused",
    "connection reset",
    "hostname lookup failed",
    "dial tcp",
    "tls",
    "closed connection",
)

DEFAULT_TERADATA_PORT = "1025"
IMPLICIT_UPPER_COLUMN_PATTERN = re.compile(r"^(?!\d)[A-Z0-9_]+$")
TABLE_EXISTS_SQL = (
    "LOCKING ROW FOR ACCESS SELECT 1 FROM DBC.TablesV "
    "WHERE UPPER(TRIM(DatabaseName)) = UPPER(TRIM(COALESCE(CAST(? AS VARCHAR(128)), DATABASE))) "
    "AND UPPER(TRIM(TableName)) = UPPER(TRIM(?)) "
    "AND TableKind IN ('T', 'O')"
)
SCHEMA_EXISTS_SQL = (
    "LOCKING ROW FOR ACCESS SELECT 1 FROM DBC.DatabasesV WHERE UPPER(TRIM(DatabaseName)) = UPPER(TRIM(?))"
)


def build_profile() -> DriverParameterProfile:
    """Build the native QMARK parameter profile."""
    return DriverParameterProfile(
        name="teradata",
        default_style=ParameterStyle.QMARK,
        supported_styles={
            ParameterStyle.QMARK,
            ParameterStyle.NUMERIC,
            ParameterStyle.NAMED_COLON,
            ParameterStyle.NAMED_PYFORMAT,
            ParameterStyle.POSITIONAL_PYFORMAT,
        },
        default_execution_style=ParameterStyle.QMARK,
        supported_execution_styles={ParameterStyle.QMARK},
        has_native_list_expansion=False,
        preserve_parameter_format=True,
        needs_static_script_compilation=False,
        allow_mixed_parameter_styles=False,
        preserve_original_params_for_many=False,
        json_serializer_strategy="helper",
        custom_type_coercions={bool: int, bytearray: bytes, memoryview: bytes, **build_uuid_coercions()},
        default_dialect="teradata",
    )


def build_statement_config(
    *, json_serializer: Callable[[Any], str] | None = None, json_deserializer: Callable[[str], Any] | None = None
) -> StatementConfig:
    """Build the Teradata statement configuration."""
    return build_statement_config_from_profile(
        driver_profile,
        statement_overrides={"dialect": "teradata"},
        json_serializer=json_serializer or to_json,
        json_deserializer=json_deserializer or from_json,
    )


def apply_driver_features(
    statement_config: StatementConfig, driver_features: Mapping[str, Any] | None
) -> tuple[StatementConfig, dict[str, Any]]:
    """Validate and apply native feature defaults."""
    features = dict(driver_features or {})
    features.setdefault("enable_lowercase_column_names", True)
    features.setdefault("arraysize", 2048)
    features.setdefault("enable_fastexport", False)
    features.setdefault("enable_fastload", False)
    size = features["arraysize"]
    if isinstance(size, bool) or not isinstance(size, int) or size < 1:
        msg = "arraysize must be an integer >= 1"
        raise ImproperConfigurationError(msg)
    if features.get("query_band") is not None:
        format_query_band(features["query_band"])
    serializer = features.setdefault("json_serializer", to_json)
    deserializer = features.setdefault("json_deserializer", from_json)
    if serializer is not None:
        statement_config = statement_config.replace(
            parameter_config=statement_config.parameter_config.with_json_serializers(
                serializer, deserializer=deserializer
            )
        )
    return statement_config, features


def build_connection_config(connection_config: Mapping[str, Any]) -> dict[str, Any]:
    """Normalize friendly connection options without changing caller mappings."""
    result = dict(connection_config)
    extra = result.pop("extra", {})
    if isinstance(extra, Mapping):
        for key, value in extra.items():
            result.setdefault(key, value)
    for key in ("pool_min_size", "pool_max_size", "pool_timeout", "pool_recycle_seconds", "health_check_interval"):
        result.pop(key, None)
    user = result.pop("username", None)
    if user is not None:
        result.setdefault("user", user)
    port = result.pop("port", result.get("dbs_port"))
    if port is not None:
        try:
            valid = not isinstance(port, bool) and 1 <= int(port) <= _MAX_PORT
        except (ValueError, TypeError):
            valid = False
        if not valid:
            msg = "Teradata port must be between 1 and 65535"
            raise ImproperConfigurationError(msg)
        result.setdefault("dbs_port", str(port))
    if set(result) != {"sJson"}:
        if result.get("host"):
            result.setdefault("dbs_port", DEFAULT_TERADATA_PORT)
        result.setdefault("charset", "UTF8")
        result.setdefault("encryptdata", "true")
    for key in ("encryptdata", "lob_support", "column_name", "teradata_values", "cop", "coplast"):
        if isinstance(result.get(key), bool):
            result[key] = str(result[key]).lower()
    return {k: v for k, v in result.items() if v is not None}


def _validate_query_band_pair(key: str, value: str) -> str:
    """Validate a single query-band key/value pair and escape SQL single quotes."""
    if not _QUERY_BAND_KEY_PATTERN.fullmatch(key) or any(c in value for c in ";\r\n\x00"):
        msg = "Invalid query band key or value"
        raise ImproperConfigurationError(msg)
    return f"{key}={value.replace(chr(39), chr(39) * 2)}"


def format_query_band(query_band: Mapping[str, str] | str) -> str:
    """Validate query-band values and escape SQL literals."""
    if isinstance(query_band, str):
        body = query_band.strip().removesuffix(";")
        if body.strip().upper() == "NONE":
            return ""
        segments = body.split(";")
        formatted: list[str] = []
        for segment in segments:
            if not segment or "=" not in segment:
                msg = "Invalid query band key or value"
                raise ImproperConfigurationError(msg)
            key, value = segment.split("=", 1)
            formatted.append(_validate_query_band_pair(key, value))
        return ";".join(formatted) + ";"
    if not query_band:
        msg = "Query band must contain key=value pairs"
        raise ImproperConfigurationError(msg)
    return ";".join(_validate_query_band_pair(str(k), str(v)) for k, v in query_band.items()) + ";"


def build_set_query_band_sql(query_band: Mapping[str, str] | str, *, transaction: bool = False) -> str:
    """Render a validated QUERY_BAND command."""
    return render_query_band_sql(format_query_band(query_band), transaction=transaction)


def render_query_band_sql(formatted_band: str, *, transaction: bool = False) -> str:
    """Render a QUERY_BAND command from a band already returned by ``format_query_band``."""
    value = f"'{formatted_band}'" if formatted_band else "NONE"
    return f"SET QUERY_BAND = {value} FOR {'TRANSACTION' if transaction else 'SESSION'};"


def is_ddl_or_session_band_sql(sql_text: str) -> bool:
    """Recognize DDL and session band requests requiring immediate commit."""
    sql = _LEADING_COMMENT_PATTERN.sub("", sql_text).upper()
    return bool(_DDL_PREFIX_PATTERN.match(sql)) or (sql.startswith("SET QUERY_BAND") and "FOR SESSION" in sql)


def maybe_prefix_fastexport(sql: str, *, enable_fastexport: bool, in_transaction: bool) -> str:
    """Prefix eligible read requests with the opt-in native FastExport escape."""
    if not enable_fastexport or in_transaction:
        return sql
    stripped = _LEADING_COMMENT_PATTERN.sub("", sql).upper()
    if stripped.startswith(("SELECT", "WITH", "LOCKING")) and "TERADATA_TRY_FASTEXPORT" not in stripped:
        return f"{{fn teradata_try_fastexport}} {sql}"
    return sql


def split_schema_and_table(identifier: str) -> tuple[str | None, str]:
    """Split a quoted database and table reference."""
    parts = split_qualified_identifier(identifier.strip(), quote_chars='"', allow_bracket_quotes=True)
    if not parts or len(parts) > _IDENTIFIER_PARTS:
        msg = "Expected a table or database.table identifier"
        raise SQLSpecError(msg)
    raw_parts = re.findall(r'"(?:[^"]|"")*"|\[(?:[^\]]|\]\])*\]|[^.\s]+', identifier.strip())
    normalized = [
        part if raw.startswith(('"', "[")) else part.upper() for part, raw in zip(parts, raw_parts, strict=True)
    ]
    return (normalized[0], normalized[1]) if len(normalized) == _IDENTIFIER_PARTS else (None, normalized[0])


def format_identifier(identifier: str) -> str:
    """Quote database-qualified identifiers."""
    parts = split_qualified_identifier(identifier.strip(), quote_chars='"', allow_bracket_quotes=True)
    if not parts or len(parts) > _IDENTIFIER_PARTS:
        msg = "Expected a table or database.table identifier"
        raise SQLSpecError(msg)
    return ".".join('"' + p.replace('"', '""') + '"' for p in parts)


def format_table_ref(table_name: str, *, allow_schema: bool = True) -> str:
    """Quote a Teradata table or database.table reference preserving catalog case rules."""
    schema_name, bare_table_name = split_schema_and_table(table_name)
    if schema_name is not None and not allow_schema:
        msg = "Schema-qualified table name is not allowed"
        raise SQLSpecError(msg)
    quoted_table = '"' + bare_table_name.replace('"', '""') + '"'
    if schema_name is not None:
        quoted_schema = '"' + schema_name.replace('"', '""') + '"'
        return f"{quoted_schema}.{quoted_table}"
    return quoted_table


def build_insert_statement(table: str, columns: list[str]) -> str:
    """Build a native parameterized INSERT."""
    names = ", ".join(format_identifier(col) for col in columns)
    marks = ", ".join("?" for _ in columns)
    return f"INSERT INTO {format_identifier(table)} ({names}) VALUES ({marks})"


def resolve_column_names(
    description: Sequence[Any] | None,
    cache: dict[tuple[int, bool], tuple[Any, list[str]]],
    *,
    enable_lowercase_column_names: bool = True,
) -> list[str]:
    """Resolve column labels, folding only implicit uppercase names."""
    if not description:
        return []
    cache_key = (id(description), enable_lowercase_column_names)
    cached = cache.get(cache_key)
    if cached is not None and cached[0] is description:
        return cached[1]
    names = [
        c[0].lower() if enable_lowercase_column_names and IMPLICIT_UPPER_COLUMN_PATTERN.fullmatch(c[0]) else c[0]
        for c in description
    ]
    if len(cache) >= _COLUMN_CACHE_SIZE:
        cache.pop(next(iter(cache)))
    cache[cache_key] = description, names
    return names


def collect_rows(
    fetched_data: Sequence[Any] | None,
    description: Sequence[Any] | None,
    cache: dict[tuple[int, bool], tuple[Any, list[str]]],
    *,
    enable_lowercase_column_names: bool = True,
) -> tuple[list[Any], list[str], Literal["dict", "tuple"]]:
    """Collect DB-API rows and retained zero-row metadata."""
    rows = list(fetched_data or ())
    return (
        rows,
        resolve_column_names(description, cache, enable_lowercase_column_names=enable_lowercase_column_names),
        "dict" if rows and isinstance(rows[0], dict) else "tuple",
    )


def resolve_rowcount(cursor: Any) -> int:
    """Resolve a nonnegative DB-API affected-row count."""
    value = getattr(cursor, "rowcount", 0)
    return max(value, 0) if isinstance(value, int) else 0


def resolve_many_rowcount(cursor: Any, parameters: Any, *, fallback_count: int | None = None) -> int:
    """Resolve bulk affected rows with payload fallback."""
    return resolve_rowcount(cursor) or (
        fallback_count if fallback_count is not None else len(parameters) if isinstance(parameters, Sized) else 0
    )


def extract_error_code(error: BaseException) -> int | None:
    """Extract the vendor database error number."""
    match = _ERROR_CODE_PATTERN.search(str(error))
    return int(match[1]) if match else None


def create_mapped_exception(error: BaseException, *, logger: Logger | None = None) -> SQLSpecError:
    """Map native database codes, retaining the original cause."""
    code = extract_error_code(error)
    cls: type[SQLSpecError] = SQLSpecError
    mapped_cls = _ERROR_CODE_MAPPING.get(code) if code is not None else None
    if mapped_cls is not None:
        cls = mapped_cls
    else:
        if code is not None and logger is not None:
            logger.debug("Unmapped Teradata error number: %s", code)
        message = str(error).lower()
        if any(marker in message for marker in _CONNECTION_ERROR_MARKERS):
            cls = DatabaseConnectionError
        elif type(error).__name__ == "OperationalError":
            cls = OperationalError
    mapped = cls(str(error))
    mapped.__cause__ = error
    return mapped


driver_profile = build_profile()
default_statement_config = build_statement_config()
