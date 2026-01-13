"""Spanner adapter compiled helpers."""

from typing import TYPE_CHECKING, Any

from google.api_core import exceptions as api_exceptions

from sqlspec.adapters.spanner.type_converter import coerce_params_for_spanner, infer_spanner_param_types
from sqlspec.core import DriverParameterProfile, ParameterStyle, StatementConfig, build_statement_config_from_profile
from sqlspec.exceptions import (
    DatabaseConnectionError,
    NotFoundError,
    OperationalError,
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
    "build_profile",
    "build_statement_config",
    "coerce_params",
    "collect_rows",
    "create_arrow_data",
    "create_mapped_exception",
    "default_statement_config",
    "driver_profile",
    "infer_param_types",
    "supports_batch_update",
    "supports_write",
)


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


def apply_driver_features(driver_features: "Mapping[str, Any] | None") -> "dict[str, Any]":
    """Apply Spanner driver feature defaults."""
    processed_features: dict[str, Any] = dict(driver_features) if driver_features else {}
    processed_features.setdefault("enable_uuid_conversion", True)
    processed_features.setdefault("json_serializer", to_json)
    processed_features.setdefault("json_deserializer", from_json)
    return processed_features


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
    rows: "Sequence[Any]", fields: "Sequence[Any]", converter: Any
) -> "tuple[list[dict[str, Any]], list[str]]":
    """Collect Spanner rows into dictionaries.

    Args:
        rows: Rows from result set.
        fields: Result set fields metadata.
        converter: Type converter for row values.

    Returns:
        Tuple of (rows, column_names).
    """
    column_names = [field.name for field in fields]
    data: list[dict[str, Any]] = []
    for row in rows:
        item: dict[str, Any] = {}
        for index, column in enumerate(column_names):
            item[column] = converter.convert_if_detected(row[index])
        data.append(item)
    return data, column_names


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


def create_mapped_exception(error: Any) -> SQLSpecError:
    """Map Spanner exceptions to SQLSpec exceptions.

    This is a factory function that returns an exception instance rather than
    raising. This pattern is more robust for use in __exit__ handlers and
    avoids issues with exception control flow in different Python versions.

    Args:
        error: The Spanner exception to map

    Returns:
        A SQLSpec exception that wraps the original error
    """
    if isinstance(error, api_exceptions.AlreadyExists):
        return _create_spanner_error(error, UniqueViolationError, "resource already exists")
    if isinstance(error, api_exceptions.NotFound):
        return _create_spanner_error(error, NotFoundError, "resource not found")
    if isinstance(error, api_exceptions.InvalidArgument):
        return _create_spanner_error(error, SQLParsingError, "invalid query or argument")
    if isinstance(error, api_exceptions.PermissionDenied):
        return _create_spanner_error(error, DatabaseConnectionError, "permission denied")
    if isinstance(error, (api_exceptions.ServiceUnavailable, api_exceptions.TooManyRequests)):
        return _create_spanner_error(error, OperationalError, "service unavailable or rate limited")
    return _create_spanner_error(error, SQLSpecError, "error")
