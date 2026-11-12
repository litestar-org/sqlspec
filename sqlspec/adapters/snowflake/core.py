"""Snowflake connection normalization, parameter profiles, and result helpers."""

import contextlib
import os
import re
from collections.abc import Callable, Mapping, Sequence
from datetime import datetime
from pathlib import Path
from typing import Any, Final, Literal
from urllib.parse import urlsplit

from sqlspec.adapters.snowflake.type_converter import SnowflakeOutputConverter
from sqlspec.core import DriverParameterProfile, ParameterStyle, StatementConfig, build_statement_config_from_profile
from sqlspec.exceptions import (
    CheckViolationError,
    DatabaseConnectionError,
    DataError,
    ForeignKeyViolationError,
    ImproperConfigurationError,
    IntegrityError,
    MissingDependencyError,
    NotFoundError,
    NotNullViolationError,
    OperationalError,
    PermissionDeniedError,
    QueryTimeoutError,
    SQLParsingError,
    SQLSpecError,
    UniqueViolationError,
)
from sqlspec.typing import import_optional
from sqlspec.utils.serializers import from_json, to_json
from sqlspec.utils.text import normalize_identifier, quote_identifier
from sqlspec.utils.type_converters import build_uuid_coercions

__all__ = (
    "DEFAULT_STAGE_PARQUET_THRESHOLD_ROWS",
    "POOL_CONFIG_KEYS",
    "apply_driver_features",
    "build_empty_arrow_table_from_description",
    "build_profile",
    "build_statement_config",
    "collect_rows",
    "connection_paramstyle",
    "create_mapped_exception",
    "default_statement_config",
    "descriptor_column_name",
    "descriptor_type_code",
    "driver_profile",
    "format_snowflake_identifier",
    "is_snowflake_error",
    "normalize_column_names",
    "normalize_connection_params",
    "normalize_execute_parameters",
    "normalize_many_parameters",
    "resolve_column_names",
    "resolve_rowcount",
    "snowflake_type_code_to_arrow_type",
)

DEFAULT_STAGE_PARQUET_THRESHOLD_ROWS: Final[int] = 1000
IMPLICIT_UPPER_COLUMN_PATTERN: Final[re.Pattern[str]] = re.compile(r"^(?!\d)[A-Z0-9_]+$")
VALID_WORKLOAD_IDENTITY_PROVIDERS: Final[frozenset[str]] = frozenset({"AWS", "GCP", "AZURE", "OIDC"})
TOKEN_AUTHENTICATORS: Final[frozenset[str]] = frozenset({"PROGRAMMATIC_ACCESS_TOKEN", "oauth", "WORKLOAD_IDENTITY"})
POOL_CONFIG_KEYS: Final[frozenset[str]] = frozenset({
    "pool_min_size",
    "pool_max_size",
    "pool_timeout",
    "pool_recycle_seconds",
    "health_check_interval",
})
_QUALIFIED_IDENTIFIER_PART: Final[re.Pattern[str]] = re.compile(r'\s*("(?:[^"]|"")*"|[A-Za-z_][A-Za-z0-9_$]*)\s*(\.|$)')
_AUTH_ALIASES: Final[dict[str, str]] = {
    "snowflake": "snowflake",
    "jwt": "SNOWFLAKE_JWT",
    "keypair": "SNOWFLAKE_JWT",
    "snowflake_jwt": "SNOWFLAKE_JWT",
    "pat": "PROGRAMMATIC_ACCESS_TOKEN",
    "programmatic_access_token": "PROGRAMMATIC_ACCESS_TOKEN",
    "workload_identity": "WORKLOAD_IDENTITY",
    "wif": "WORKLOAD_IDENTITY",
    "oauth": "oauth",
    "externalbrowser": "externalbrowser",
    "mfa": "username_password_mfa",
    "username_password_mfa": "username_password_mfa",
}
_COLUMN_CACHE_MAX_SIZE: Final[int] = 256
_FORBIDDEN_ERRNO: Final[int] = 250005
_OBJECT_NOT_FOUND_ERRNO: Final[int] = 2003
_QUERY_CANCELLED_ERRNO: Final[int] = 604
_CONNECTION_ERRNOS: Final[frozenset[int]] = frozenset({250001, 250002, 250003, 250004, 250006})
_MAX_INT64_DECIMAL_PRECISION: Final[int] = 18
_TIME_TYPE_CODE: Final[int] = 12
_TYPED_BINDING_SIZE: Final[int] = 2
_TYPED_BINDING_TYPE_NAMES: Final[frozenset[str]] = frozenset({
    "ARRAY",
    "BINARY",
    "BOOLEAN",
    "DATE",
    "FIXED",
    "GEOGRAPHY",
    "GEOMETRY",
    "OBJECT",
    "REAL",
    "TEXT",
    "TIME",
    "TIMESTAMP",
    "TIMESTAMP_LTZ",
    "TIMESTAMP_NTZ",
    "TIMESTAMP_TZ",
    "VARIANT",
    "VECTOR",
})


class _SnowflakeTupleConverter:
    __slots__ = ("_serializer",)

    def __init__(self, serializer: Callable[[Any], str]) -> None:
        self._serializer = serializer

    def __call__(self, value: Any) -> Any:
        if (
            len(value) == _TYPED_BINDING_SIZE
            and isinstance(value[0], str)
            and value[0].upper() in _TYPED_BINDING_TYPE_NAMES
        ):
            return value
        return self._serializer(list(value))


def _with_server_tuple_bindings(statement_config: StatementConfig, serializer: Callable[[Any], str]) -> StatementConfig:
    if statement_config.parameter_config.default_execution_parameter_style not in {
        ParameterStyle.QMARK,
        ParameterStyle.POSITIONAL_COLON,
    }:
        return statement_config
    type_map = dict(statement_config.parameter_config.type_coercion_map)
    type_map[tuple] = _SnowflakeTupleConverter(serializer)
    return statement_config.replace(
        parameter_config=statement_config.parameter_config.replace(type_coercion_map=type_map)
    )


def build_profile(*, paramstyle: str = "qmark") -> DriverParameterProfile:
    """Build a Snowflake parameter profile for the connector binding mode."""
    if paramstyle == "qmark":
        execution_style = ParameterStyle.QMARK
        execution_styles = {ParameterStyle.QMARK}
    elif paramstyle in {"pyformat", "format"}:
        execution_style = ParameterStyle.POSITIONAL_PYFORMAT
        execution_styles = {ParameterStyle.POSITIONAL_PYFORMAT, ParameterStyle.NAMED_PYFORMAT}
    elif paramstyle == "numeric":
        execution_style = ParameterStyle.POSITIONAL_COLON
        execution_styles = {ParameterStyle.POSITIONAL_COLON}
    else:
        msg = "Snowflake paramstyle must be qmark, numeric, pyformat, or format"
        raise ImproperConfigurationError(msg)
    return DriverParameterProfile(
        name="Snowflake",
        default_style=ParameterStyle.QMARK,
        supported_styles={
            ParameterStyle.QMARK,
            ParameterStyle.NAMED_COLON,
            ParameterStyle.POSITIONAL_COLON,
            ParameterStyle.NAMED_PYFORMAT,
            ParameterStyle.POSITIONAL_PYFORMAT,
        },
        default_execution_style=execution_style,
        supported_execution_styles=execution_styles,
        has_native_list_expansion=False,
        preserve_parameter_format=True,
        needs_static_script_compilation=False,
        allow_mixed_parameter_styles=False,
        preserve_original_params_for_many=False,
        json_serializer_strategy="helper",
        custom_type_coercions=build_uuid_coercions(),
        default_dialect="snowflake",
    )


def build_statement_config(
    *,
    paramstyle: str = "qmark",
    json_serializer: Callable[[Any], str] | None = None,
    json_deserializer: Callable[[str], Any] | None = None,
) -> StatementConfig:
    """Construct Snowflake statement settings and JSON parameter codecs."""
    statement_config = build_statement_config_from_profile(
        build_profile(paramstyle=paramstyle),
        statement_overrides={"dialect": "snowflake"},
        json_serializer=json_serializer or to_json,
        json_deserializer=json_deserializer or from_json,
    )
    return _with_server_tuple_bindings(statement_config, json_serializer or to_json)


def apply_driver_features(
    statement_config: StatementConfig | None, driver_features: Mapping[str, Any] | None, *, paramstyle: str = "qmark"
) -> tuple[StatementConfig, dict[str, Any]]:
    """Apply Snowflake codec and bulk-transfer defaults to independent settings."""
    features = dict(driver_features or {})
    serializer = features.setdefault("json_serializer", to_json)
    deserializer = features.setdefault("json_deserializer", from_json)
    for key, default in (
        ("enable_lowercase_column_names", True),
        ("enable_stage_parquet_load", True),
        ("stage_parquet_threshold_rows", DEFAULT_STAGE_PARQUET_THRESHOLD_ROWS),
        ("enable_json_deserialization", True),
        ("enable_query_tag_correlation", False),
        ("enable_events", True),
        ("events_backend", "poll_queue"),
    ):
        features.setdefault(key, default)
    threshold = features["stage_parquet_threshold_rows"]
    if isinstance(threshold, bool) or not isinstance(threshold, int) or threshold <= 0:
        msg = "Snowflake stage_parquet_threshold_rows must be a positive integer"
        raise ImproperConfigurationError(msg)
    if statement_config is None:
        return build_statement_config(
            paramstyle=paramstyle, json_serializer=serializer, json_deserializer=deserializer
        ), features
    statement_config = statement_config.replace(
        parameter_config=statement_config.parameter_config.with_json_serializers(serializer, deserializer=deserializer)
    )
    return _with_server_tuple_bindings(statement_config, serializer), features


def _normalize_authenticator(value: str) -> str:
    parsed = urlsplit(value)
    host = parsed.hostname or ""
    if parsed.scheme == "https" and any(
        host.endswith(suffix) for suffix in (".okta.com", ".oktapreview.com", ".okta-emea.com")
    ):
        return value
    result = _AUTH_ALIASES.get(value.lower())
    if result is None:
        msg = "Unsupported Snowflake authentication type"
        raise ImproperConfigurationError(msg)
    return result


def _normalize_authentication(params: dict[str, Any]) -> None:
    """Resolve authenticator aliases and reject conflicting login modes."""
    auth_type = params.pop("auth_type", None)
    authenticator = params.get("authenticator")
    if auth_type is not None:
        if str(auth_type).lower() == "okta":
            if authenticator is None or not str(authenticator).startswith("https://"):
                msg = "Snowflake Okta authentication requires an HTTPS authenticator URL"
                raise ImproperConfigurationError(msg)
            params["authenticator"] = _normalize_authenticator(str(authenticator))
        else:
            selected = _normalize_authenticator(str(auth_type))
            if authenticator is not None and selected != _normalize_authenticator(str(authenticator)):
                msg = "Snowflake auth_type and authenticator conflict"
                raise ImproperConfigurationError(msg)
            params["authenticator"] = selected
    elif authenticator is not None:
        params["authenticator"] = _normalize_authenticator(str(authenticator))


def _normalize_private_key(params: dict[str, Any]) -> None:
    """Normalize key objects, file paths, and encrypted key passphrases."""
    passphrase = params.pop("private_key_passphrase", None)
    password = passphrase.encode("utf-8") if isinstance(passphrase, str) else passphrase
    key = params.pop("private_key", None)
    if isinstance(key, Path) or (isinstance(key, str) and not key.lstrip().startswith("-----BEGIN")):
        params["private_key_file"] = str(key)
    elif isinstance(key, bytes):
        params["private_key"] = key
    elif key is not None:
        serialization = import_optional("cryptography.hazmat.primitives.serialization")
        if serialization is None:
            raise MissingDependencyError(package="cryptography", install_package="snowflake")
        if isinstance(key, str):
            key = serialization.load_pem_private_key(key.encode("utf-8"), password=password)
        if not callable(getattr(key, "private_bytes", None)):
            msg = "Snowflake private_key must be DER bytes, PEM text, a path, or a private key object"
            raise ImproperConfigurationError(msg)
        params["private_key"] = key.private_bytes(
            serialization.Encoding.DER, serialization.PrivateFormat.PKCS8, serialization.NoEncryption()
        )
    if params.get("private_key_file") is not None:
        params["private_key_file"] = str(params["private_key_file"])
        if password is not None:
            file_password = password
            if isinstance(password, bytes):
                with contextlib.suppress(UnicodeDecodeError):
                    file_password = password.decode("utf-8")
            params.setdefault("private_key_file_pwd", file_password)
    if "private_key" in params or "private_key_file" in params:
        params.setdefault("authenticator", "SNOWFLAKE_JWT")


def normalize_connection_params(connection_config: Mapping[str, Any]) -> dict[str, Any]:
    """Normalize authentication and session options for a fresh physical connection."""
    params = {
        key: value for key, value in connection_config.items() if value is not None and key not in POOL_CONFIG_KEYS
    }
    username = params.pop("username", None)
    if username is not None:
        params.setdefault("user", username)
    extra = params.pop("extra", {})
    if not isinstance(extra, Mapping):
        msg = "Snowflake extra must be a mapping"
        raise ImproperConfigurationError(msg)
    for key, value in extra.items():
        if value is not None and key not in POOL_CONFIG_KEYS:
            params.setdefault(key, value)
    _normalize_authentication(params)
    _normalize_private_key(params)
    provider = params.get("workload_identity_provider")
    if provider is not None:
        provider = str(provider).upper()
        if provider not in VALID_WORKLOAD_IDENTITY_PROVIDERS:
            msg = "Snowflake workload_identity_provider must be AWS, GCP, AZURE, or OIDC"
            raise ImproperConfigurationError(msg)
        params["workload_identity_provider"] = provider
        params.setdefault("authenticator", "WORKLOAD_IDENTITY")
    impersonation = params.get("workload_identity_impersonation_path")
    if impersonation is not None:
        params["workload_identity_impersonation_path"] = (
            [impersonation] if isinstance(impersonation, str) else list(impersonation)
        )
    token_provider = params.pop("auth_token_provider", None)
    if token_provider is not None:
        if not callable(token_provider):
            msg = "Snowflake auth_token_provider must be callable"
            raise ImproperConfigurationError(msg)
        token = token_provider()
        if not isinstance(token, str) or not token:
            msg = "Snowflake auth_token_provider must return a nonempty string"
            raise ImproperConfigurationError(msg)
        params["token" if params.get("authenticator") in TOKEN_AUTHENTICATORS else "password"] = token
    for key_name in ("token_file_path", "connections_file_path"):
        if key_name in params:
            params[key_name] = str(params[key_name])
    if not any(params.get(key_name) for key_name in ("connection_name", "account", "host")):
        default_name = os.environ.get("SNOWFLAKE_DEFAULT_CONNECTION_NAME")
        if default_name:
            params["connection_name"] = default_name
    session = params.pop("session_parameters", None)
    if session is not None and not isinstance(session, Mapping):
        msg = "Snowflake session_parameters must be a mapping"
        raise ImproperConfigurationError(msg)
    session_parameters = dict(session or {})
    for key_name, session_name in (("query_tag", "QUERY_TAG"), ("timezone", "TIMEZONE")):
        value = params.pop(key_name, None)
        if value is not None:
            session_parameters.setdefault(session_name, value)
    if session_parameters:
        params["session_parameters"] = session_parameters
    params.setdefault("paramstyle", "qmark")
    if params["paramstyle"] not in {"qmark", "pyformat", "format", "numeric"}:
        msg = "Snowflake paramstyle must be qmark, numeric, pyformat, or format"
        raise ImproperConfigurationError(msg)
    return params


def normalize_column_names(column_names: Sequence[str], *, enable_lowercase_column_names: bool = True) -> list[str]:
    """Fold implicit uppercase names and retain explicitly mixed-case aliases."""
    return [
        name.lower() if enable_lowercase_column_names and IMPLICIT_UPPER_COLUMN_PATTERN.fullmatch(name) else name
        for name in column_names
    ]


def descriptor_column_name(column: Any) -> str:
    """Extract the raw column name from a DB-API tuple or ResultMetadata descriptor."""
    return str(column.name if hasattr(column, "name") else column[0])


def descriptor_type_code(column: Any) -> int | None:
    """Extract the Snowflake type code from a DB-API tuple or ResultMetadata descriptor."""
    raw = column.type_code if hasattr(column, "type_code") else column[1]
    return int(raw) if raw is not None else None


def resolve_column_names(
    description: Sequence[Any] | None,
    cache: dict[tuple[tuple[str, ...], bool], list[str]],
    *,
    enable_lowercase_column_names: bool = True,
) -> list[str]:
    """Resolve result column names with a bounded descriptor-tuple cache."""
    if not description:
        return []
    raw_names = tuple(descriptor_column_name(column) for column in description)
    cache_key = (raw_names, enable_lowercase_column_names)
    cached = cache.get(cache_key)
    if cached is not None:
        return cached
    names = normalize_column_names(raw_names, enable_lowercase_column_names=enable_lowercase_column_names)
    if len(cache) >= _COLUMN_CACHE_MAX_SIZE:
        cache.pop(next(iter(cache)))
    cache[cache_key] = names
    return names


def collect_rows(
    fetched_data: Sequence[Any] | None,
    description: Sequence[Any] | None,
    cache: dict[tuple[tuple[str, ...], bool], list[str]],
    *,
    converter: SnowflakeOutputConverter | None = None,
    enable_lowercase_column_names: bool = True,
) -> tuple[list[tuple[Any, ...]], list[str], Literal["tuple"]]:
    """Collect normalized positional rows and decode semi-structured fields using cursor metadata."""
    names = resolve_column_names(description, cache, enable_lowercase_column_names=enable_lowercase_column_names)
    rows = list(fetched_data or [])
    if not rows:
        return [], names, "tuple"
    raw_names = tuple(descriptor_column_name(column) for column in description) if description else ()
    tuple_rows: list[tuple[Any, ...]] = []
    for row in rows:
        if isinstance(row, Mapping):
            if raw_names:
                tuple_rows.append(
                    tuple(
                        row[raw_name] if raw_name in row else row.get(name)
                        for raw_name, name in zip(raw_names, names, strict=True)
                    )
                )
            else:
                tuple_rows.append(tuple(row.values()))
        else:
            tuple_rows.append(tuple(row))
    if converter is None or not description:
        return tuple_rows, names, "tuple"
    type_codes = tuple(descriptor_type_code(column) for column in description)
    converted = [
        tuple(
            converter.convert_value(value, type_code=type_code)
            for value, type_code in zip(row, type_codes, strict=True)
        )
        for row in tuple_rows
    ]
    return converted, names, "tuple"


def resolve_rowcount(cursor: Any) -> int:
    """Return a known nonnegative cursor row count, or minus one."""
    count = getattr(cursor, "rowcount", None)
    return int(count) if count is not None and count >= 0 else -1


def _normalize_server_binding(value: Any) -> Any:
    if isinstance(value, datetime) and value.tzinfo is not None and value.utcoffset() is not None:
        return ("TIMESTAMP_TZ", value)
    return value


def _normalize_server_parameters(parameters: Any) -> Any:
    if isinstance(parameters, Mapping):
        return {key: _normalize_server_binding(value) for key, value in parameters.items()}
    if isinstance(parameters, tuple):
        return tuple(_normalize_server_binding(value) for value in parameters)
    if isinstance(parameters, list):
        return [_normalize_server_binding(value) for value in parameters]
    return _normalize_server_binding(parameters)


def normalize_execute_parameters(parameters: Any, *, paramstyle: str = "qmark") -> Any:
    """Preserve aware datetime offsets for server bindings and omit empty bindings."""
    if parameters is None or (isinstance(parameters, (list, tuple, dict)) and not parameters):
        return None
    return _normalize_server_parameters(parameters) if paramstyle in {"qmark", "numeric"} else parameters


def normalize_many_parameters(parameters: Any, *, paramstyle: str = "qmark") -> list[Any]:
    """Materialize batches and preserve aware datetime offsets for server bindings."""
    rows = list(parameters) if parameters is not None else []
    return [_normalize_server_parameters(row) for row in rows] if paramstyle in {"qmark", "numeric"} else rows


def is_snowflake_error(error: BaseException) -> bool:
    """Recognize connector exceptions without importing the optional SDK eagerly."""
    errors = import_optional("snowflake.connector.errors")
    error_type = getattr(errors, "Error", None)
    return isinstance(error, error_type) if isinstance(error_type, type) else False


def create_mapped_exception(error: BaseException) -> SQLSpecError:
    """Map Snowflake error metadata without requiring the optional connector."""
    name = type(error).__name__
    errno = getattr(error, "errno", None)
    sqlstate = str(getattr(error, "sqlstate", None) or "")
    exception_type: type[SQLSpecError] = OperationalError
    if name == "ForbiddenError" or errno == _FORBIDDEN_ERRNO:
        exception_type = PermissionDeniedError
    elif errno == _OBJECT_NOT_FOUND_ERRNO:
        exception_type = NotFoundError
    elif errno == _QUERY_CANCELLED_ERRNO or sqlstate == "57014":
        exception_type = QueryTimeoutError
    elif errno in _CONNECTION_ERRNOS or sqlstate.startswith("08"):
        exception_type = DatabaseConnectionError
    elif sqlstate in {"23505", "23503", "23502", "23514"}:
        exception_type = {
            "23505": UniqueViolationError,
            "23503": ForeignKeyViolationError,
            "23502": NotNullViolationError,
            "23514": CheckViolationError,
        }[sqlstate]
    elif sqlstate.startswith("23") or name == "IntegrityError":
        exception_type = IntegrityError
    elif sqlstate == "42501" or sqlstate.startswith("28"):
        exception_type = PermissionDeniedError
    elif sqlstate.startswith("42") or name == "ProgrammingError":
        exception_type = SQLParsingError
    elif sqlstate.startswith("22") or name == "DataError":
        exception_type = DataError
    elif name == "InterfaceError":
        exception_type = DatabaseConnectionError
    message = str(getattr(error, "msg", None) or error)
    query_id = getattr(error, "sfqid", None)
    if query_id:
        message += f" [sfqid={query_id}]"
    mapped = exception_type(message)
    mapped.__cause__ = error
    return mapped


def snowflake_type_code_to_arrow_type(
    type_code: int | None, precision: int | None = None, scale: int | None = None
) -> Any:
    """Map Snowflake result metadata to an Arrow type without fetching rows."""
    pa = import_optional("pyarrow")
    if pa is None:
        raise MissingDependencyError(package="pyarrow", install_package="snowflake")
    if type_code == 0:
        normalized_precision = 38 if precision is None else max(1, min(38, int(precision)))
        normalized_scale = 0 if scale is None else max(0, min(normalized_precision, int(scale)))
        if precision is not None and normalized_scale == 0 and normalized_precision <= _MAX_INT64_DECIMAL_PRECISION:
            return pa.int64()
        return pa.decimal128(normalized_precision, normalized_scale)
    factories = {
        1: pa.float64,
        2: pa.string,
        3: pa.date32,
        5: pa.string,
        9: pa.string,
        10: pa.string,
        11: pa.binary,
        13: pa.bool_,
    }
    if type_code in factories:
        return factories[type_code]()
    if type_code == _TIME_TYPE_CODE:
        return pa.time64("ns")
    if type_code in {4, 6, 7, 8}:
        return pa.timestamp("ns", tz="UTC" if type_code in {6, 8} else None)
    return pa.string()


def build_empty_arrow_table_from_description(
    description: Sequence[Any] | None, driver_features: Mapping[str, Any]
) -> Any:
    """Preserve empty result names and scalar types from DB-API metadata."""
    pa = import_optional("pyarrow")
    if pa is None:
        raise MissingDependencyError(package="pyarrow", install_package="snowflake")
    columns = description or []
    names = normalize_column_names(
        [descriptor_column_name(column) for column in columns],
        enable_lowercase_column_names=bool(driver_features.get("enable_lowercase_column_names", True)),
    )
    fields = []
    for name, column in zip(names, columns, strict=True):
        if hasattr(column, "type_code"):
            type_code, precision, scale = column.type_code, column.precision, column.scale
            nullable = column.is_nullable
        else:
            type_code, precision, scale, nullable = column[1], column[4], column[5], column[6]
        fields.append(
            pa.field(
                name,
                snowflake_type_code_to_arrow_type(type_code, precision, scale),
                nullable=bool(nullable) if nullable is not None else True,
            )
        )
    return pa.Table.from_batches([], schema=pa.schema(fields))


driver_profile = build_profile()
default_statement_config = build_statement_config()


def format_snowflake_identifier(identifier: str) -> str:
    """Validate a 1- to 3-part identifier and quote each part under Snowflake folding.

    Args:
        identifier: Bare or double-quoted identifier, optionally database- and schema-qualified.

    Returns:
        Dot-joined, double-quoted identifier with bare parts folded to uppercase.

    Raises:
        SQLSpecError: If the identifier is empty or not a valid identifier sequence.
    """
    position = 0
    parts: list[str] = []
    while position < len(identifier):
        matched = _QUALIFIED_IDENTIFIER_PART.match(identifier, position)
        if matched is None:
            msg = "Invalid Snowflake identifier"
            raise SQLSpecError(msg)
        parts.append(quote_identifier(normalize_identifier(matched.group(1), "snowflake")))
        position = matched.end()
        if matched.group(2) == "." and position == len(identifier):
            msg = "Invalid Snowflake identifier"
            raise SQLSpecError(msg)
    if not parts:
        msg = "Snowflake identifier must not be empty"
        raise SQLSpecError(msg)
    return ".".join(parts)


def connection_paramstyle(connection: Any) -> str:
    """Return the paramstyle a connector connection binds parameters with.

    Args:
        connection: Snowflake connector connection.

    Returns:
        ``qmark``, ``numeric``, ``pyformat``, or ``format``; ``qmark`` when the connection reports none.
    """
    style = str(getattr(connection, "paramstyle", None) or getattr(connection, "_paramstyle", None) or "qmark").lower()
    return style if style in {"qmark", "numeric", "pyformat", "format"} else "qmark"
