"""DuckDB adapter compiled helpers."""

from datetime import date, datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any, cast

from sqlspec.adapters.duckdb.type_converter import DuckDBOutputConverter
from sqlspec.core import DriverParameterProfile, ParameterStyle, StatementConfig, build_statement_config_from_profile
from sqlspec.utils.serializers import to_json
from sqlspec.utils.type_converters import build_decimal_converter, build_time_iso_converter

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping


__all__ = (
    "apply_duckdb_driver_features",
    "build_duckdb_profile",
    "build_duckdb_statement_config",
    "coerce_duckdb_rows",
    "duckdb_statement_config",
)


_TIME_TO_ISO = build_time_iso_converter()
_DECIMAL_TO_STRING = build_decimal_converter(mode="string")


def _bool_to_int(value: bool) -> int:
    return int(value)


def coerce_duckdb_rows(fetched_data: "list[Any]", column_names: "list[str]") -> "list[dict[str, Any]] | list[Any]":
    """Convert row tuples into dictionaries keyed by column names.

    Args:
        fetched_data: Raw rows returned from DuckDB.
        column_names: Column names from cursor metadata.

    Returns:
        List of dictionaries when rows are tuple-based, otherwise the original rows.
    """
    if fetched_data and isinstance(fetched_data[0], tuple):
        return [dict(zip(column_names, row, strict=False)) for row in fetched_data]
    return fetched_data


def build_duckdb_profile() -> "DriverParameterProfile":
    """Create the DuckDB driver parameter profile."""

    return DriverParameterProfile(
        name="DuckDB",
        default_style=ParameterStyle.QMARK,
        supported_styles={ParameterStyle.QMARK, ParameterStyle.NUMERIC, ParameterStyle.NAMED_DOLLAR},
        default_execution_style=ParameterStyle.QMARK,
        supported_execution_styles={ParameterStyle.QMARK},
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
        },
        default_dialect="duckdb",
    )


def apply_duckdb_driver_features(
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


def build_duckdb_statement_config(*, json_serializer: "Callable[[Any], str] | None" = None) -> StatementConfig:
    """Construct the DuckDB statement configuration with optional JSON serializer."""
    serializer = json_serializer or to_json
    return build_statement_config_from_profile(
        build_duckdb_profile(), statement_overrides={"dialect": "duckdb"}, json_serializer=serializer
    )


duckdb_statement_config = build_duckdb_statement_config()
