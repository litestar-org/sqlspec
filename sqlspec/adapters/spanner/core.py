"""Spanner adapter compiled helpers."""

from functools import partial
from typing import TYPE_CHECKING, Any, cast

from google.api_core import exceptions as api_exceptions
from google.cloud.spanner_v1.data_types import JsonObject
from google.cloud.spanner_v1.types.type import TypeCode

from sqlspec.adapters.spanner.type_converter import coerce_params_for_spanner, infer_spanner_param_types
from sqlspec.core import DriverParameterProfile, ParameterStyle, StatementConfig, build_statement_config_from_profile
from sqlspec.exceptions import (
    DeadlockError,
    NotFoundError,
    OperationalError,
    PermissionDeniedError,
    QueryTimeoutError,
    SQLParsingError,
    SQLSpecError,
    UniqueViolationError,
)
from sqlspec.utils.arrow_helpers import convert_dict_to_arrow
from sqlspec.utils.serializers import from_json, to_json

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping, Sequence

    from sqlspec.typing import ArrowRecordBatch, ArrowRecordBatchReader, ArrowReturnFormat, ArrowTable

__all__ = (
    "apply_driver_features",
    "build_param_type_signature",
    "build_profile",
    "build_statement_config",
    "coerce_params",
    "collect_rows",
    "create_arrow_data",
    "create_mapped_exception",
    "default_statement_config",
    "driver_profile",
    "infer_param_types",
    "resolve_column_names",
    "resolve_row_plan",
    "supports_batch_update",
    "supports_write",
)

COLUMN_CACHE_MAX_SIZE: int = 128


def build_profile() -> "DriverParameterProfile":
    """Create the Spanner driver parameter profile."""

    return DriverParameterProfile(
        name="Spanner",
        default_style=ParameterStyle.NAMED_AT,
        supported_styles={ParameterStyle.NAMED_AT},
        default_execution_style=ParameterStyle.NAMED_AT,
        supported_execution_styles={ParameterStyle.NAMED_AT},
        has_native_list_expansion=True,
        json_serializer_strategy="none",
        default_dialect="spanner",
        preserve_parameter_format=True,
        needs_static_script_compilation=False,
        allow_mixed_parameter_styles=False,
        preserve_original_params_for_many=True,
        custom_type_coercions=None,
        extras={},
    )


driver_profile = build_profile()


def build_statement_config() -> StatementConfig:
    """Construct the Spanner statement configuration."""
    profile = driver_profile
    return build_statement_config_from_profile(profile, statement_overrides={"dialect": "spanner"})


default_statement_config = build_statement_config()


def apply_driver_features(
    statement_config: "StatementConfig", driver_features: "Mapping[str, Any] | None"
) -> "tuple[StatementConfig, dict[str, Any]]":
    """Apply Spanner driver feature defaults."""
    processed_features: dict[str, Any] = dict(driver_features) if driver_features else {}
    processed_features.setdefault("enable_uuid_conversion", True)
    processed_features.setdefault("json_serializer", to_json)
    processed_features.setdefault("json_deserializer", from_json)
    return statement_config, processed_features


def supports_write(cursor: Any) -> bool:
    """Return True when the cursor supports DML execution."""
    try:
        _ = cursor.execute_update
    except AttributeError:
        return False
    return True


def supports_batch_update(cursor: Any) -> bool:
    """Return True when the cursor supports batch updates."""
    try:
        _ = cursor.batch_update
    except AttributeError:
        return False
    return True


def infer_param_types(params: "dict[str, Any] | list[Any] | tuple[Any, ...] | None") -> "dict[str, Any]":
    """Infer Spanner param_types from Python values."""
    if not isinstance(params, dict):
        return {}
    return infer_spanner_param_types(params)


def build_param_type_signature(params: "dict[str, Any] | None") -> "tuple[tuple[str, type[Any]], ...]":
    """Build a hashable signature for Spanner param type inference caching.

    Args:
        params: Coerced parameter mapping.

    Returns:
        Tuple signature based on parameter keys and value runtime types.
    """
    if not params:
        return ()
    return tuple((key, type(value)) for key, value in params.items())


def resolve_column_names(fields: "Sequence[Any] | None", cache: "dict[int, tuple[Any, list[str]]]") -> list[str]:
    """Resolve and cache Spanner field names for row materialization."""
    if not fields:
        return []

    cache_key = id(fields)
    cached = cache.get(cache_key)
    if cached is not None and cached[0] is fields:
        return cached[1]

    column_names = [field.name for field in fields]
    if len(cache) >= COLUMN_CACHE_MAX_SIZE:
        cache.pop(next(iter(cache)))
    cache[cache_key] = (fields, column_names)
    return column_names


def _convert_json_row_value(value: Any, *, json_deserializer: "Callable[[str], Any]") -> Any:
    """Convert a native Spanner JSON cell using the configured deserializer."""
    if isinstance(value, JsonObject):
        json_value = cast("Any", value).serialize()
    elif isinstance(value, str):
        json_value = value
    else:
        return value

    try:
        return json_deserializer(json_value)
    except (TypeError, ValueError):
        return value


def resolve_row_plan(
    fields: "Sequence[Any] | None",
    cache: "dict[int, tuple[Any, list[str], tuple[tuple[int, Any], ...] | None]]",
    *,
    json_deserializer: "Callable[[str], Any]",
) -> "tuple[list[str], tuple[tuple[int, Any], ...] | None]":
    """Resolve column names and metadata-driven converters for a Spanner result."""
    if not fields:
        return [], None

    cache_key = id(fields)
    cached = cache.get(cache_key)
    if cached is not None and cached[0] is fields:
        return cached[1], cached[2]

    column_names = [getattr(field, "name", "") for field in fields]
    plan_entries: list[tuple[int, Any]] = []
    append_plan_entry = plan_entries.append
    convert_json = partial(_convert_json_row_value, json_deserializer=json_deserializer)
    for index, field in enumerate(fields):
        field_type = getattr(field, "type_", None)
        type_code = getattr(field_type, "code", None)
        if type_code == TypeCode.JSON:
            append_plan_entry((index, convert_json))

    plan = tuple(plan_entries) if plan_entries else None
    if len(cache) >= COLUMN_CACHE_MAX_SIZE:
        cache.pop(next(iter(cache)))
    cache[cache_key] = (fields, column_names, plan)
    return column_names, plan


def coerce_params(
    params: "dict[str, Any] | list[Any] | tuple[Any, ...] | None",
    *,
    json_serializer: "Callable[[Any], str] | None" = None,
) -> "dict[str, Any] | None":
    """Coerce Python types to Spanner-compatible formats."""
    if not isinstance(params, dict):
        return None
    return coerce_params_for_spanner(params, json_serializer=json_serializer)


def collect_rows(
    rows: "list[Any]",
    fields: "Sequence[Any]",
    *,
    column_names: "list[str] | None" = None,
    column_plan: "tuple[tuple[int, Any], ...] | None" = None,
) -> "tuple[list[Any], list[str]]":
    """Collect Spanner rows as tuples with type conversion applied.

    When a metadata-driven column plan is unavailable, the original row list
    is returned unchanged. When a plan exists, only the planned columns are
    converted and the rows are copied once.

    Args:
        rows: Rows from result set.
        fields: Result set fields metadata.
        column_names: Optional precomputed column names.
        column_plan: Optional zero-copy column conversion plan.

    Returns:
        Tuple of (rows, column_names).
    """
    resolved_column_names = column_names if column_names is not None else [field.name for field in fields]
    if column_plan is None and all(type(row) is tuple for row in rows):
        return rows, resolved_column_names
    if column_plan is None:
        return [tuple(row) for row in rows], resolved_column_names

    data: list[Any] = []
    append = data.append
    for row in rows:
        converted_row = list(row)
        for index, converter in column_plan:
            converted_row[index] = converter(converted_row[index])
        append(tuple(converted_row))
    return data, resolved_column_names


def create_arrow_data(
    data: "list[dict[str, Any]]", return_format: "ArrowReturnFormat"
) -> "ArrowTable | ArrowRecordBatch | ArrowRecordBatchReader | list[ArrowRecordBatch]":
    """Create Arrow data from Spanner row dictionaries.

    Args:
        data: Row dictionaries from Spanner results.
        return_format: Arrow return format.

    Returns:
        Arrow data in the requested format.
    """
    return convert_dict_to_arrow(data, return_format=return_format)


def _create_spanner_error(error: Any, error_class: type[SQLSpecError], description: str) -> SQLSpecError:
    """Create a Spanner error instance without raising it."""
    msg = f"Spanner {description}: {error}"
    exc = error_class(msg)
    exc.__cause__ = error
    return exc


def create_mapped_exception(error: Any, *, logger: Any | None = None) -> SQLSpecError:
    """Map Spanner exceptions to SQLSpec exceptions.

    This is a factory function that returns an exception instance rather than
    raising. This pattern is more robust for use in __exit__ handlers and
    avoids issues with exception control flow in different Python versions.

    Mapping priority:
        1. Native google.api_core exception types (isinstance checks)
        2. Default SQLSpecError fallback

    Args:
        error: The Spanner exception to map
        logger: Optional logger accepted for adapter signature parity.

    Returns:
        A SQLSpec exception that wraps the original error
    """
    del logger
    # Integrity errors
    if isinstance(error, api_exceptions.AlreadyExists):
        return _create_spanner_error(error, UniqueViolationError, "resource already exists")

    # Resource not found
    if isinstance(error, api_exceptions.NotFound):
        return _create_spanner_error(error, NotFoundError, "resource not found")

    # SQL/argument errors
    if isinstance(error, api_exceptions.InvalidArgument):
        return _create_spanner_error(error, SQLParsingError, "invalid query or argument")

    # Permission/authentication errors
    if isinstance(error, api_exceptions.PermissionDenied):
        return _create_spanner_error(error, PermissionDeniedError, "permission denied")
    if isinstance(error, api_exceptions.Unauthenticated):
        return _create_spanner_error(error, PermissionDeniedError, "authentication failed")

    # Transaction errors (deadlock/abort)
    if isinstance(error, api_exceptions.Aborted):
        return _create_spanner_error(error, DeadlockError, "transaction aborted")

    # Query timeout/cancellation
    if isinstance(error, api_exceptions.Cancelled):
        return _create_spanner_error(error, QueryTimeoutError, "operation cancelled")
    if isinstance(error, api_exceptions.DeadlineExceeded):
        return _create_spanner_error(error, QueryTimeoutError, "deadline exceeded")

    # Service/operational errors
    if isinstance(error, (api_exceptions.ServiceUnavailable, api_exceptions.TooManyRequests)):
        return _create_spanner_error(error, OperationalError, "service unavailable or rate limited")

    return _create_spanner_error(error, SQLSpecError, "error")
