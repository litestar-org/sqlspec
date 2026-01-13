"""ADBC adapter compiled helpers."""

import datetime
import decimal
from typing import TYPE_CHECKING, Any, cast

from sqlspec.adapters.adbc.type_converter import ADBCOutputConverter
from sqlspec.core import (
    DriverParameterProfile,
    ParameterStyle,
    StatementConfig,
    build_null_pruning_transform,
    build_statement_config_from_profile,
)
from sqlspec.exceptions import (
    CheckViolationError,
    DatabaseConnectionError,
    DataError,
    ForeignKeyViolationError,
    ImproperConfigurationError,
    IntegrityError,
    NotNullViolationError,
    SQLParsingError,
    SQLSpecError,
    TransactionError,
    UniqueViolationError,
)
from sqlspec.typing import Empty
from sqlspec.utils.module_loader import import_string
from sqlspec.utils.serializers import to_json
from sqlspec.utils.type_guards import has_rowcount, has_sqlstate

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping

    from sqlspec.core import SQL

__all__ = (
    "apply_driver_features",
    "build_connection_config",
    "build_profile",
    "collect_rows",
    "create_mapped_exception",
    "detect_dialect",
    "driver_from_uri",
    "driver_kind_from_driver_name",
    "driver_kind_from_uri",
    "driver_profile",
    "get_statement_config",
    "handle_postgres_rollback",
    "is_postgres_dialect",
    "normalize_driver_path",
    "normalize_postgres_empty_parameters",
    "normalize_script_rowcount",
    "prepare_parameters_with_casts",
    "prepare_postgres_parameters",
    "resolve_dialect_from_config",
    "resolve_dialect_from_driver_path",
    "resolve_dialect_name",
    "resolve_driver_connect_func",
    "resolve_driver_name",
    "resolve_driver_name_from_config",
    "resolve_parameter_casts",
    "resolve_parameter_styles",
    "resolve_rowcount",
)

DIALECT_PATTERNS: "dict[str, tuple[str, ...]]" = {
    "postgres": ("postgres", "postgresql"),
    "bigquery": ("bigquery",),
    "sqlite": ("sqlite", "flight", "flightsql"),
    "duckdb": ("duckdb", "gizmosql", "gizmo"),
    "mysql": ("mysql",),
    "snowflake": ("snowflake",),
}


DIALECT_PARAMETER_STYLES: "dict[str, tuple[ParameterStyle, list[ParameterStyle]]]" = {
    "postgres": (ParameterStyle.NUMERIC, [ParameterStyle.NUMERIC]),
    "postgresql": (ParameterStyle.NUMERIC, [ParameterStyle.NUMERIC]),
    "bigquery": (ParameterStyle.NAMED_AT, [ParameterStyle.NAMED_AT]),
    "sqlite": (ParameterStyle.QMARK, [ParameterStyle.QMARK]),
    "duckdb": (ParameterStyle.QMARK, [ParameterStyle.QMARK, ParameterStyle.NUMERIC, ParameterStyle.NAMED_DOLLAR]),
    "mysql": (ParameterStyle.POSITIONAL_PYFORMAT, [ParameterStyle.POSITIONAL_PYFORMAT, ParameterStyle.NAMED_PYFORMAT]),
    "snowflake": (ParameterStyle.QMARK, [ParameterStyle.QMARK, ParameterStyle.NUMERIC]),
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
)

_DRIVER_PATH_KEYWORDS_TO_DIALECT: "tuple[tuple[str, str], ...]" = (
    ("postgresql", "postgres"),
    ("sqlite", "sqlite"),
    ("duckdb", "duckdb"),
    ("bigquery", "bigquery"),
    ("snowflake", "snowflake"),
    ("gizmosql", "duckdb"),
    ("flightsql", "sqlite"),
    ("grpc", "sqlite"),
)

_PARAMETER_STYLES_BY_KEYWORD: "tuple[tuple[str, tuple[tuple[str, ...], str]], ...]" = (
    ("postgresql", (("numeric",), "numeric")),
    ("sqlite", (("qmark", "named_colon"), "qmark")),
    ("duckdb", (("qmark", "numeric"), "qmark")),
    ("gizmosql", (("qmark", "numeric"), "qmark")),
    ("bigquery", (("named_at",), "named_at")),
    ("snowflake", (("qmark", "numeric"), "qmark")),
)

_BIGQUERY_DB_KWARGS_FIELDS: "tuple[str, ...]" = ("project_id", "dataset_id", "token")


def detect_dialect(connection: Any, logger: Any | None = None) -> str:
    """Detect database dialect from ADBC driver information.

    Args:
        connection: ADBC connection with driver metadata.
        logger: Optional logger for diagnostics.

    Returns:
        Detected dialect name, defaulting to ``postgres``.
    """
    try:
        driver_info = connection.adbc_get_info()
        vendor_name = driver_info.get("vendor_name", "").lower()
        driver_name = driver_info.get("driver_name", "").lower()

        for dialect, patterns in DIALECT_PATTERNS.items():
            for pattern in patterns:
                if pattern in vendor_name or pattern in driver_name:
                    if logger is not None:
                        logger.debug("Dialect detected: %s (from %s/%s)", dialect, vendor_name, driver_name)
                    return dialect
    except Exception as exc:
        if logger is not None:
            logger.debug("Dialect detection failed: %s", exc)

    if logger is not None:
        logger.warning("Could not determine dialect from driver info. Defaulting to 'postgres'.")
    return "postgres"


def normalize_driver_path(driver_name: str) -> str:
    """Normalize a driver name to an importable connect function path."""
    stripped = driver_name.strip()
    if stripped.endswith(".dbapi.connect"):
        return stripped
    if stripped.endswith(".dbapi"):
        return f"{stripped}.connect"
    if "." in stripped:
        return stripped
    return f"{stripped}.dbapi.connect"


def driver_from_uri(uri: str) -> "str | None":
    """Resolve a default driver connect path from a URI."""
    for prefix, driver_path in _URI_PREFIX_DRIVER:
        if uri.startswith(prefix):
            return driver_path
    return None


def driver_kind_from_driver_name(driver_name: str) -> "str | None":
    """Return a canonical driver kind based on driver name content."""
    lowered_name = driver_name.lower()
    if lowered_name in {"gizmosql", "gizmo"}:
        return "gizmosql"
    resolved = _DRIVER_ALIASES.get(driver_name.lower(), driver_name)
    lowered = resolved.lower()
    for keyword, _dialect in _DRIVER_PATH_KEYWORDS_TO_DIALECT:
        if keyword in lowered:
            return keyword
    return None


def driver_kind_from_uri(uri: str) -> "str | None":
    """Return a canonical driver kind based on URI scheme."""
    lowered = uri.lower()
    if lowered.startswith(("gizmosql://", "gizmo://", "grpc+tls://")):
        return "gizmosql"
    for prefix, driver_path in _URI_PREFIX_DRIVER:
        if uri.startswith(prefix):
            return driver_kind_from_driver_name(driver_path)
    return None


def resolve_driver_name(driver_name: str | None, uri: str | None) -> str:
    """Resolve and normalize the driver name."""
    if isinstance(driver_name, str):
        lowered_driver = driver_name.lower()
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
    return resolve_driver_name(connection_config.get("driver_name"), connection_config.get("uri"))


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
    """Get the SQL dialect type based on the driver path."""
    for keyword, dialect in _DRIVER_PATH_KEYWORDS_TO_DIALECT:
        if keyword in driver_path:
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

    driver_name = connection_config.get("driver_name")
    if isinstance(driver_name, str) and driver_name.lower() in {"gizmosql", "gizmo"}:
        return "duckdb"

    uri = connection_config.get("uri")
    if isinstance(uri, str):
        lowered_uri = uri.lower()
        if lowered_uri.startswith(("gizmosql://", "gizmo://", "grpc+tls://")):
            return "duckdb"

    return resolve_dialect_from_driver_path(resolve_driver_name_from_config(connection_config))


def resolve_parameter_styles(driver_path: str | None, logger: Any | None = None) -> "tuple[tuple[str, ...], str]":
    """Resolve supported and default parameter styles from a driver path.

    Args:
        driver_path: Import path for the ADBC driver connect function.
        logger: Optional logger for diagnostics.

    Returns:
        Tuple of (supported_parameter_styles, default_parameter_style)
    """
    if isinstance(driver_path, str):
        lowered = driver_path.lower()
        for keyword, styles in _PARAMETER_STYLES_BY_KEYWORD:
            if keyword in lowered:
                return styles
    if logger is not None:
        logger.debug("Error resolving parameter styles, using defaults")
    return (("qmark",), "qmark")


def build_connection_config(connection_config: "Mapping[str, Any]") -> "dict[str, Any]":
    """Build a normalized connection configuration dictionary.

    Args:
        connection_config: Raw connection configuration mapping.

    Returns:
        Normalized connection configuration dictionary.
    """
    config = dict(connection_config)

    driver_name = config.get("driver_name")
    uri = config.get("uri")
    driver_kind: str | None = None
    if isinstance(driver_name, str):
        driver_kind = driver_kind_from_driver_name(driver_name)
    if driver_kind is None and isinstance(uri, str):
        driver_kind = driver_kind_from_uri(uri)

    if isinstance(uri, str) and driver_kind == "sqlite" and uri.startswith("sqlite://"):
        config["uri"] = uri[9:]
    if isinstance(uri, str) and driver_kind == "duckdb" and uri.startswith("duckdb://"):
        config["path"] = uri[9:]
        config.pop("uri", None)

    if isinstance(driver_name, str) and driver_kind == "bigquery":
        db_kwargs = config.get("db_kwargs")
        db_kwargs_dict: dict[str, Any] = dict(db_kwargs) if isinstance(db_kwargs, dict) else {}
        for param in _BIGQUERY_DB_KWARGS_FIELDS:
            if param in config:
                db_kwargs_dict[param] = config.pop(param)
        if db_kwargs_dict:
            config["db_kwargs"] = db_kwargs_dict
    elif isinstance(driver_name, str) and "db_kwargs" in config and driver_kind != "bigquery":
        db_kwargs = config.pop("db_kwargs")
        if isinstance(db_kwargs, dict):
            config.update(db_kwargs)

    config.pop("driver_name", None)

    return config


def resolve_dialect_name(dialect: Any) -> str:
    """Return the normalized dialect name string."""
    if dialect is None:
        return ""
    return str(dialect)


def is_postgres_dialect(dialect_name: str) -> bool:
    """Return True when the dialect indicates PostgreSQL."""
    return dialect_name in {"postgres", "postgresql"}


def handle_postgres_rollback(dialect: str, cursor: Any, logger: Any | None = None) -> None:
    """Execute rollback for PostgreSQL after transaction failure.

    Args:
        dialect: Active dialect identifier.
        cursor: Database cursor to execute rollback.
        logger: Optional logger for diagnostics.
    """
    if dialect != "postgres":
        return
    try:
        cursor.execute("ROLLBACK")
    except Exception:
        return
    if logger is not None:
        logger.debug("PostgreSQL rollback executed after transaction failure")


def normalize_postgres_empty_parameters(dialect: str, parameters: Any) -> Any:
    """Normalize empty parameter payloads for PostgreSQL drivers.

    Args:
        dialect: Active dialect identifier.
        parameters: Parameter payload in any supported shape.

    Returns:
        Normalized parameter payload.
    """
    if dialect == "postgres" and isinstance(parameters, dict) and not parameters:
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
    return prepare_parameters_with_casts(
        postgres_compatible, parameter_casts, statement_config, dialect=dialect, json_serializer=json_serializer
    )


def _create_adbc_error(error: Any, error_class: type[SQLSpecError], description: str) -> SQLSpecError:
    """Create an ADBC error instance without raising it."""
    msg = f"ADBC {description}: {error}"
    exc = error_class(msg)
    exc.__cause__ = error
    return exc


def create_mapped_exception(error: Any) -> SQLSpecError:
    """Map ADBC errors to SQLSpec exceptions.

    This is a factory function that returns an exception instance rather than
    raising. This pattern is more robust for use in __exit__ handlers and
    avoids issues with exception control flow in different Python versions.

    Args:
        error: The ADBC exception to map

    Returns:
        A SQLSpec exception that wraps the original error
    """
    sqlstate = error.sqlstate if has_sqlstate(error) and error.sqlstate is not None else None

    if sqlstate:
        if sqlstate == "23505":
            return _create_adbc_error(error, UniqueViolationError, "unique constraint violation")
        if sqlstate == "23503":
            return _create_adbc_error(error, ForeignKeyViolationError, "foreign key constraint violation")
        if sqlstate == "23502":
            return _create_adbc_error(error, NotNullViolationError, "not-null constraint violation")
        if sqlstate == "23514":
            return _create_adbc_error(error, CheckViolationError, "check constraint violation")
        if sqlstate.startswith("23"):
            return _create_adbc_error(error, IntegrityError, "integrity constraint violation")
        if sqlstate.startswith("42"):
            return _create_adbc_error(error, SQLParsingError, "SQL parsing error")
        if sqlstate.startswith("08"):
            return _create_adbc_error(error, DatabaseConnectionError, "connection error")
        if sqlstate.startswith("40"):
            return _create_adbc_error(error, TransactionError, "transaction error")
        if sqlstate.startswith("22"):
            return _create_adbc_error(error, DataError, "data error")
        return _create_adbc_error(error, SQLSpecError, "database error")

    error_msg = str(error).lower()

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
    if "syntax" in error_msg:
        return _create_adbc_error(error, SQLParsingError, "SQL parsing error")
    if "connection" in error_msg or "connect" in error_msg:
        return _create_adbc_error(error, DatabaseConnectionError, "connection error")
    return _create_adbc_error(error, SQLSpecError, "database error")


def _identity(value: Any) -> Any:
    return value


def _convert_array_for_postgres_adbc(value: Any) -> Any:
    """Convert array values for PostgreSQL compatibility."""

    if isinstance(value, tuple):
        return list(value)
    return value


def _get_type_coercion_map(dialect: str) -> "dict[type, Any]":
    """Return dialect-aware type coercion mapping for Arrow parameter handling."""

    return {
        datetime.datetime: _identity,
        datetime.date: _identity,
        datetime.time: _identity,
        decimal.Decimal: float,
        bool: _identity,
        int: _identity,
        float: _identity,
        bytes: _identity,
        tuple: _convert_array_for_postgres_adbc,
        list: _convert_array_for_postgres_adbc,
        dict: _identity,
    }


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
            datetime.datetime: _identity,
            datetime.date: _identity,
            datetime.time: _identity,
            decimal.Decimal: float,
            bool: _identity,
            int: _identity,
            float: _identity,
            bytes: _identity,
            tuple: _convert_array_for_postgres_adbc,
            list: _convert_array_for_postgres_adbc,
            dict: _identity,
        },
        extras={
            "type_coercion_overrides": {list: _convert_array_for_postgres_adbc, tuple: _convert_array_for_postgres_adbc}
        },
    )


driver_profile = build_profile()


def get_statement_config(detected_dialect: str) -> StatementConfig:
    """Create statement configuration for the specified dialect."""
    default_style, supported_styles = DIALECT_PARAMETER_STYLES.get(
        detected_dialect, (ParameterStyle.QMARK, [ParameterStyle.QMARK])
    )

    type_map = _get_type_coercion_map(detected_dialect)

    sqlglot_dialect = "postgres" if detected_dialect == "postgresql" else detected_dialect
    profile = driver_profile

    parameter_overrides: dict[str, Any] = {
        "default_parameter_style": default_style,
        "supported_parameter_styles": set(supported_styles),
        "default_execution_parameter_style": default_style,
        "supported_execution_parameter_styles": set(supported_styles),
        "type_coercion_map": type_map,
    }

    if detected_dialect == "duckdb":
        parameter_overrides["preserve_parameter_format"] = False
        parameter_overrides["supported_execution_parameter_styles"] = {ParameterStyle.QMARK, ParameterStyle.NUMERIC}

    if detected_dialect in {"postgres", "postgresql"}:
        parameter_overrides["ast_transformer"] = build_null_pruning_transform(dialect=sqlglot_dialect)

    return build_statement_config_from_profile(
        profile, parameter_overrides=parameter_overrides, statement_overrides={"dialect": sqlglot_dialect}
    )


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

    json_serializer = cast("Callable[[Any], str] | None", processed_features.setdefault("json_serializer", to_json))
    processed_features.setdefault("enable_cast_detection", True)
    processed_features.setdefault("strict_type_coercion", False)
    processed_features.setdefault("enable_strict_type_coercion", processed_features["strict_type_coercion"])
    processed_features.setdefault("arrow_extension_types", True)
    processed_features.setdefault("enable_arrow_extension_types", processed_features["arrow_extension_types"])

    if json_serializer is not None:
        statement_config = _apply_adbc_json_serializer(statement_config, json_serializer)

    return statement_config, processed_features


def collect_rows(
    fetched_data: "list[Any] | None", description: "list[Any] | None"
) -> "tuple[list[dict[str, Any]], list[str]]":
    """Collect ADBC rows into dictionaries with column names.

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
    if isinstance(fetched_data[0], tuple):
        dict_rows = [dict(zip(column_names, row, strict=False)) for row in fetched_data]
        return dict_rows, column_names
    return cast("list[dict[str, Any]]", fetched_data), column_names


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
        result: list[Any] = []
        converter = ADBCOutputConverter(dialect)
        for idx, param in enumerate(parameters, start=1):
            cast_type = parameter_casts.get(idx, "").upper()
            if cast_type in {"JSON", "JSONB", "TYPE.JSON", "TYPE.JSONB"}:
                if isinstance(param, dict):
                    result.append(json_encoder(param))
                else:
                    result.append(param)
            elif isinstance(param, dict):
                result.append(converter.convert_dict(param))
            else:
                if statement_config.parameter_config.type_coercion_map:
                    for type_check, converter_func in statement_config.parameter_config.type_coercion_map.items():
                        if type_check is not dict and isinstance(param, type_check):
                            param = converter_func(param)
                            break
                result.append(param)
        return tuple(result) if isinstance(parameters, tuple) else result
    return parameters
