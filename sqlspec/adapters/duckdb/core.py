"""DuckDB adapter compiled helpers."""

from datetime import date, datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Final, cast

from sqlspec.adapters.duckdb.type_converter import DuckDBOutputConverter
from sqlspec.core import DriverParameterProfile, ParameterStyle, StatementConfig, build_statement_config_from_profile
from sqlspec.exceptions import (
    CheckViolationError,
    DataError,
    ForeignKeyViolationError,
    IntegrityError,
    NotFoundError,
    NotNullViolationError,
    OperationalError,
    PermissionDeniedError,
    QueryTimeoutError,
    SQLParsingError,
    SQLSpecError,
    UniqueViolationError,
)
from sqlspec.utils.serializers import to_json
from sqlspec.utils.type_converters import build_decimal_converter, build_uuid_coercions, time_iso_convert
from sqlspec.utils.type_guards import has_rowcount

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping


__all__ = (
    "apply_driver_features",
    "build_connection_config",
    "build_profile",
    "build_statement_config",
    "collect_rows",
    "create_mapped_exception",
    "default_statement_config",
    "driver_profile",
    "normalize_execute_parameters",
    "resolve_rowcount",
)


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


def build_connection_config(connection_config: "Mapping[str, Any]") -> "dict[str, Any]":
    """Build connection configuration for pool creation.

    Args:
        connection_config: Raw connection configuration mapping.

    Returns:
        Dictionary with connection parameters.
    """
    excluded_keys = {
        "pool_min_size",
        "pool_max_size",
        "pool_timeout",
        "pool_recycle_seconds",
        "health_check_interval",
        "extra",
    }
    return {key: value for key, value in connection_config.items() if value is not None and key not in excluded_keys}


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
        if result and isinstance(result, tuple) and len(result) == 1:
            return int(result[0])
    except Exception:
        if has_rowcount(cursor):
            return max(cursor.rowcount, 0)
        return 0
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
            bool: _bool_to_int,
            datetime: _TIME_TO_ISO,
            date: _TIME_TO_ISO,
            Decimal: _DECIMAL_TO_STRING,
            **build_uuid_coercions(),
        },
        default_dialect="duckdb",
    )


def _bool_to_int(value: bool) -> int:
    return int(value)


driver_profile = build_profile()


def apply_driver_features(
    statement_config: "StatementConfig", driver_features: "Mapping[str, Any] | None"
) -> "StatementConfig":
    """Apply DuckDB-specific driver features to statement configuration."""
    if not driver_features:
        return statement_config

    param_config = statement_config.parameter_config
    json_serializer = driver_features.get("json_serializer")
    if json_serializer:
        param_config = param_config.with_json_serializers(
            cast("Callable[[Any], str]", json_serializer), tuple_strategy="tuple"
        )

    enable_uuid_conversion = driver_features.get("enable_uuid_conversion", True)
    if not enable_uuid_conversion:
        type_converter = DuckDBOutputConverter(enable_uuid_conversion=enable_uuid_conversion)
        type_coercion_map = dict(param_config.type_coercion_map)
        type_coercion_map[str] = type_converter.convert_if_detected
        param_config = param_config.replace(type_coercion_map=type_coercion_map)

    if param_config is statement_config.parameter_config:
        return statement_config
    return statement_config.replace(parameter_config=param_config)


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


_EXCEPTION_MAPPING: Final[dict[type[BaseException], tuple[type[SQLSpecError], str]]] = {}
_EXCEPTION_MAPPING_CACHE: Final[dict[type[BaseException], tuple[type[SQLSpecError], str]]] = {}
_CONSTRAINT_EXCEPTION_TYPE: type[BaseException] | None = None


def _register_duckdb_exception_mappings() -> None:
    """Populate the native-type dispatch table from the installed duckdb module.

    Falls back silently when duckdb isn't importable so the substring-based
    fallback in create_mapped_exception still works for tests and probe paths.
    """
    try:
        import duckdb as _duckdb_module
    except ImportError:
        return

    global _CONSTRAINT_EXCEPTION_TYPE
    constraint_cls = getattr(_duckdb_module, "ConstraintException", None)
    if isinstance(constraint_cls, type) and issubclass(constraint_cls, BaseException):
        _CONSTRAINT_EXCEPTION_TYPE = constraint_cls

    direct_mappings: tuple[tuple[str, tuple[type[SQLSpecError], str]], ...] = (
        ("CatalogException", (NotFoundError, "catalog error")),
        ("ParserException", (SQLParsingError, "SQL parsing error")),
        ("BinderException", (SQLParsingError, "SQL parsing error")),
        ("PermissionException", (PermissionDeniedError, "permission denied")),
        ("InterruptException", (QueryTimeoutError, "query interrupted")),
        ("IOException", (OperationalError, "operational error")),
        ("ConversionException", (DataError, "data error")),
    )
    for attr_name, target in direct_mappings:
        cls = getattr(_duckdb_module, attr_name, None)
        if isinstance(cls, type) and issubclass(cls, BaseException):
            _EXCEPTION_MAPPING[cls] = target


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


_register_duckdb_exception_mappings()


def create_mapped_exception(exc_type: "type[BaseException]", error: "BaseException") -> SQLSpecError:
    """Map DuckDB exceptions to SQLSpec exceptions.

    This is a factory function that returns an exception instance rather than
    raising. This pattern is more robust for use in __exit__ handlers and
    avoids issues with exception control flow in different Python versions.

    Mapping priority:
        1. ConstraintException -> message-pattern sub-classification (Unique/FK/NotNull/Check)
        2. Native DuckDB exception type via dispatch table (MRO-walked, cached)
        3. Type-name substring fallback (for environments without duckdb importable)
        4. Message-pattern fallback for unrelated types (permission/interrupt/type-mismatch)
        5. Default SQLSpecError fallback

    Args:
        exc_type: The exception type (class)
        error: The DuckDB exception to map

    Returns:
        A SQLSpec exception that wraps the original error
    """
    if _CONSTRAINT_EXCEPTION_TYPE is not None and isinstance(error, _CONSTRAINT_EXCEPTION_TYPE):
        return _classify_duckdb_constraint(error)

    mapped = _resolve_duckdb_exception_mapping(exc_type)
    if mapped is not None:
        error_class, description = mapped
        return _create_duckdb_error(error, error_class, description)

    exc_name = exc_type.__name__.lower()
    if "constraintexception" in exc_name:
        return _classify_duckdb_constraint(error)
    if "catalogexception" in exc_name:
        return _create_duckdb_error(error, NotFoundError, "catalog error")
    if "parserexception" in exc_name or "binderexception" in exc_name:
        return _create_duckdb_error(error, SQLParsingError, "SQL parsing error")
    if "permissionexception" in exc_name:
        return _create_duckdb_error(error, PermissionDeniedError, "permission denied")
    if "interruptexception" in exc_name:
        return _create_duckdb_error(error, QueryTimeoutError, "query interrupted")
    if "ioexception" in exc_name:
        return _create_duckdb_error(error, OperationalError, "operational error")
    if "conversionexception" in exc_name:
        return _create_duckdb_error(error, DataError, "data error")

    error_msg = str(error).lower()
    if "permission denied" in error_msg or "access denied" in error_msg:
        return _create_duckdb_error(error, PermissionDeniedError, "permission denied")
    if "interrupt" in error_msg or "cancel" in error_msg:
        return _create_duckdb_error(error, QueryTimeoutError, "query canceled")
    if "type mismatch" in error_msg:
        return _create_duckdb_error(error, DataError, "data error")

    return _create_duckdb_error(error, SQLSpecError, "database error")


def build_statement_config(*, json_serializer: "Callable[[Any], str] | None" = None) -> StatementConfig:
    """Construct the DuckDB statement configuration with optional JSON serializer."""
    serializer = json_serializer or to_json
    profile = driver_profile
    return build_statement_config_from_profile(
        profile, statement_overrides={"dialect": "duckdb"}, json_serializer=serializer
    )


default_statement_config = build_statement_config()
