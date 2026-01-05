"""ADBC adapter compiled helpers."""

import datetime
import decimal
from typing import Any

from sqlspec.core import DriverParameterProfile, ParameterStyle

__all__ = ("get_type_coercion_map",)


def _identity(value: Any) -> Any:
    return value


def _convert_array_for_postgres_adbc(value: Any) -> Any:
    """Convert array values for PostgreSQL compatibility."""

    if isinstance(value, tuple):
        return list(value)
    return value


def get_type_coercion_map(dialect: str) -> "dict[type, Any]":
    """Return dialect-aware type coercion mapping for Arrow parameter handling."""

    return {
        datetime.datetime: lambda x: x,
        datetime.date: lambda x: x,
        datetime.time: lambda x: x,
        decimal.Decimal: float,
        bool: lambda x: x,
        int: lambda x: x,
        float: lambda x: x,
        bytes: lambda x: x,
        tuple: _convert_array_for_postgres_adbc,
        list: _convert_array_for_postgres_adbc,
        dict: lambda x: x,
    }


def _build_adbc_profile() -> "DriverParameterProfile":
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
