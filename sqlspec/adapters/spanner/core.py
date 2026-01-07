"""Spanner adapter compiled helpers."""

from typing import TYPE_CHECKING, Any

from sqlspec.adapters.spanner.type_converter import coerce_params_for_spanner, infer_spanner_param_types
from sqlspec.core import DriverParameterProfile, ParameterStyle, StatementConfig, build_statement_config_from_profile
from sqlspec.utils.serializers import from_json, to_json

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping

__all__ = (
    "apply_spanner_driver_features",
    "build_spanner_profile",
    "build_spanner_statement_config",
    "coerce_spanner_params",
    "collect_spanner_rows",
    "infer_spanner_param_types_for_params",
    "spanner_statement_config",
    "supports_spanner_batch_update",
    "supports_spanner_write",
)


def build_spanner_profile() -> "DriverParameterProfile":
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


def build_spanner_statement_config() -> StatementConfig:
    """Construct the Spanner statement configuration."""
    profile = build_spanner_profile()
    return build_statement_config_from_profile(profile, statement_overrides={"dialect": "spanner"})


spanner_statement_config = build_spanner_statement_config()


def apply_spanner_driver_features(driver_features: "Mapping[str, Any] | None") -> "dict[str, Any]":
    """Apply Spanner driver feature defaults."""
    processed_features: dict[str, Any] = dict(driver_features) if driver_features else {}
    processed_features.setdefault("enable_uuid_conversion", True)
    processed_features.setdefault("json_serializer", to_json)
    processed_features.setdefault("json_deserializer", from_json)
    return processed_features


def supports_spanner_write(cursor: Any) -> bool:
    """Return True when the cursor supports DML execution."""
    try:
        _ = cursor.execute_update
    except AttributeError:
        return False
    return True


def supports_spanner_batch_update(cursor: Any) -> bool:
    """Return True when the cursor supports batch updates."""
    try:
        _ = cursor.batch_update
    except AttributeError:
        return False
    return True


def infer_spanner_param_types_for_params(params: "dict[str, Any] | None") -> "dict[str, Any]":
    """Infer Spanner param_types from Python values."""
    if isinstance(params, (list, tuple)):
        return {}
    return infer_spanner_param_types(params)


def coerce_spanner_params(
    params: "dict[str, Any] | None", *, json_serializer: "Callable[[Any], str] | None" = None
) -> "dict[str, Any] | None":
    """Coerce Python types to Spanner-compatible formats."""
    if isinstance(params, (list, tuple)):
        return None
    return coerce_params_for_spanner(params, json_serializer=json_serializer)


def collect_spanner_rows(
    rows: "list[Any]", fields: "list[Any]", converter: Any
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
