"""DuckDB adapter compiled helpers."""

from datetime import date, datetime
from decimal import Decimal

from sqlspec.core import DriverParameterProfile, ParameterStyle
from sqlspec.utils.type_converters import build_decimal_converter, build_time_iso_converter

_TIME_TO_ISO = build_time_iso_converter()
_DECIMAL_TO_STRING = build_decimal_converter(mode="string")


def _bool_to_int(value: bool) -> int:
    return int(value)


def _build_duckdb_profile() -> "DriverParameterProfile":
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
