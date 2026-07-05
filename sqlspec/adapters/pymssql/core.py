"""pymssql adapter compiled helpers."""

import re
from collections.abc import Callable, Sized
from typing import TYPE_CHECKING, Any, Final

from sqlspec.core import DriverParameterProfile, ParameterStyle, StatementConfig, build_statement_config_from_profile
from sqlspec.exceptions import (
    DatabaseConnectionError,
    DataError,
    DeadlockError,
    ForeignKeyViolationError,
    IntegrityError,
    NotNullViolationError,
    OperationalError,
    PermissionDeniedError,
    QueryTimeoutError,
    SQLSpecError,
    UniqueViolationError,
)
from sqlspec.utils.serializers import from_json, to_json
from sqlspec.utils.text import split_qualified_identifier
from sqlspec.utils.type_converters import build_uuid_coercions
from sqlspec.utils.type_guards import has_rowcount

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence
    from logging import Logger

__all__ = (
    "apply_driver_features",
    "build_insert_statement",
    "build_profile",
    "build_statement_config",
    "collect_rows",
    "create_mapped_exception",
    "default_statement_config",
    "driver_profile",
    "format_identifier",
    "normalize_execute_many_parameters",
    "normalize_execute_parameters",
    "resolve_column_names",
    "resolve_many_rowcount",
    "resolve_rowcount",
)

_ERROR_NUMBER_PATTERN: Final[re.Pattern[str]] = re.compile(r"\(([-]?\d+)\)")
_ERROR_CODE_MAPPING: Final[dict[int, tuple[type[SQLSpecError], str]]] = {
    2601: (UniqueViolationError, "unique constraint violation"),
    2627: (UniqueViolationError, "unique constraint violation"),
    547: (ForeignKeyViolationError, "foreign key or check constraint violation"),
    515: (NotNullViolationError, "not-null constraint violation"),
    18456: (PermissionDeniedError, "permission denied"),
    4060: (DatabaseConnectionError, "database connection error"),
    53: (DatabaseConnectionError, "database connection error"),
    1205: (DeadlockError, "deadlock detected"),
    -2: (QueryTimeoutError, "query timeout"),
    8114: (DataError, "data conversion error"),
    1105: (OperationalError, "operational error"),
}


def format_identifier(identifier: str) -> str:
    """Format a T-SQL identifier with bracket quoting."""
    cleaned = identifier.strip()
    if not cleaned:
        msg = "Table name must not be empty"
        raise SQLSpecError(msg)
    parts = split_qualified_identifier(cleaned, quote_chars='"', allow_bracket_quotes=True)
    return ".".join(_quote_bracket_identifier(part) for part in parts)


def build_insert_statement(table: str, columns: "list[str]") -> str:
    """Build a pymssql-compatible INSERT statement."""
    column_clause = ", ".join(_quote_bracket_identifier(column) for column in columns)
    placeholders = ", ".join("%s" for _ in columns)
    return f"INSERT INTO {format_identifier(table)} ({column_clause}) VALUES ({placeholders})"


def normalize_execute_parameters(parameters: Any) -> Any:
    """Normalize parameters for pymssql execute calls."""
    if parameters is None:
        return None
    if isinstance(parameters, list):
        return tuple(parameters)
    return parameters


def normalize_execute_many_parameters(parameters: Any) -> Any:
    """Normalize parameters for pymssql executemany calls."""
    if not parameters:
        msg = "execute_many requires parameters"
        raise ValueError(msg)
    return parameters


def _bool_to_int(value: bool) -> int:
    return int(value)


def _custom_type_coercions() -> dict[type, Callable[[Any], Any]]:
    """Return custom type coercions for pymssql."""
    coercions: dict[type, Callable[[Any], Any]] = {bool: _bool_to_int}
    coercions.update(build_uuid_coercions())
    return coercions


def build_profile() -> "DriverParameterProfile":
    """Create the pymssql driver parameter profile."""
    return DriverParameterProfile(
        name="pymssql",
        default_style=ParameterStyle.QMARK,
        supported_styles={ParameterStyle.QMARK, ParameterStyle.NAMED_PYFORMAT},
        default_execution_style=ParameterStyle.POSITIONAL_PYFORMAT,
        supported_execution_styles={ParameterStyle.POSITIONAL_PYFORMAT, ParameterStyle.NAMED_PYFORMAT},
        has_native_list_expansion=False,
        preserve_parameter_format=True,
        needs_static_script_compilation=False,
        allow_mixed_parameter_styles=False,
        preserve_original_params_for_many=False,
        json_serializer_strategy="helper",
        custom_type_coercions=_custom_type_coercions(),
        default_dialect="tsql",
    )


driver_profile = build_profile()


def build_statement_config(
    *, json_serializer: "Callable[[Any], str] | None" = None, json_deserializer: "Callable[[str], Any] | None" = None
) -> "StatementConfig":
    """Construct the pymssql statement configuration."""
    return build_statement_config_from_profile(
        driver_profile,
        statement_overrides={"dialect": "tsql"},
        json_serializer=json_serializer or to_json,
        json_deserializer=json_deserializer or from_json,
    )


default_statement_config = build_statement_config()


def apply_driver_features(
    statement_config: "StatementConfig", driver_features: "Mapping[str, Any] | None"
) -> "tuple[StatementConfig, dict[str, Any]]":
    """Apply pymssql driver feature defaults to statement config."""
    features: dict[str, Any] = dict(driver_features) if driver_features else {}
    json_serializer = features.setdefault("json_serializer", to_json)
    json_deserializer = features.setdefault("json_deserializer", from_json)

    if json_serializer is not None:
        parameter_config = statement_config.parameter_config.with_json_serializers(
            json_serializer, deserializer=json_deserializer
        )
        statement_config = statement_config.replace(parameter_config=parameter_config)

    return statement_config, features


def create_mapped_exception(error: Exception, *, logger: "Logger | None" = None) -> SQLSpecError:
    """Map a pymssql exception to SQLSpec's exception hierarchy."""
    error_number = _extract_error_number(error)
    if error_number is not None:
        mapping = _ERROR_CODE_MAPPING.get(error_number)
        if mapping is not None:
            error_class, description = mapping
            return error_class(f"SQL Server error {error_number}: {description}. Original error: {error}")
        if logger is not None:
            logger.debug("Unmapped SQL Server error number: %s", error_number)

    exc_name = type(error).__name__
    if exc_name == "IntegrityError":
        return IntegrityError(f"SQL Server integrity error. Original error: {error}")
    if exc_name == "OperationalError":
        return OperationalError(f"SQL Server operational error. Original error: {error}")
    if exc_name == "DataError":
        return DataError(f"SQL Server data error. Original error: {error}")
    return SQLSpecError(f"SQL Server database error. Original error: {error}")


def resolve_column_names(description: "Sequence[Any] | None") -> "list[str]":
    """Resolve ordered column names from cursor metadata."""
    if not description:
        return []
    return [desc[0] for desc in description]


def collect_rows(
    fetched_data: "Sequence[Any] | None", description: "Sequence[Any] | None"
) -> "tuple[list[Any], list[str], str]":
    """Collect pymssql rows, preserving tuple row shape."""
    column_names = resolve_column_names(description)
    if not fetched_data:
        return [], column_names, "tuple"
    return list(fetched_data), column_names, "tuple"


def resolve_rowcount(cursor: Any) -> int:
    """Resolve rowcount from a pymssql cursor."""
    if not has_rowcount(cursor):
        return 0
    rowcount = cursor.rowcount
    if isinstance(rowcount, int) and rowcount >= 0:
        return rowcount
    return 0


def resolve_many_rowcount(cursor: Any, parameters: Any, *, fallback_count: "int | None" = None) -> int:
    """Resolve executemany rowcount using cursor metadata with payload fallback."""
    rowcount = resolve_rowcount(cursor)
    if rowcount > 0:
        return rowcount
    if fallback_count is not None:
        return fallback_count
    if isinstance(parameters, Sized):
        return len(parameters)
    return 0


def _quote_bracket_identifier(identifier: str) -> str:
    cleaned = identifier.strip()
    if cleaned.startswith("[") and cleaned.endswith("]"):
        cleaned = cleaned[1:-1].replace("]]", "]")
    return f"[{cleaned.replace(']', ']]')}]"


def _extract_error_number(exc: Exception) -> "int | None":
    matches = _ERROR_NUMBER_PATTERN.findall(str(exc))
    if not matches:
        return None
    try:
        return int(matches[-1])
    except ValueError:
        return None
