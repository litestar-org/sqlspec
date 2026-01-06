"""AsyncPG adapter compiled helpers."""

import datetime
from typing import TYPE_CHECKING, Any

from sqlspec.core import DriverParameterProfile, ParameterStyle
from sqlspec.utils.serializers import from_json

if TYPE_CHECKING:
    from collections.abc import Callable

    from sqlspec.core import ParameterStyleConfig

__all__ = ("build_asyncpg_profile", "configure_asyncpg_parameter_serializers")


def _convert_datetime_param(value: Any) -> Any:
    """Convert datetime parameter, handling ISO strings."""

    if isinstance(value, str):
        return datetime.datetime.fromisoformat(value)
    return value


def _convert_date_param(value: Any) -> Any:
    """Convert date parameter, handling ISO strings."""

    if isinstance(value, str):
        return datetime.date.fromisoformat(value)
    return value


def _convert_time_param(value: Any) -> Any:
    """Convert time parameter, handling ISO strings."""

    if isinstance(value, str):
        return datetime.time.fromisoformat(value)
    return value


def _build_asyncpg_custom_type_coercions() -> "dict[type, Callable[[Any], Any]]":
    """Return custom type coercions for AsyncPG."""

    return {
        datetime.datetime: _convert_datetime_param,
        datetime.date: _convert_date_param,
        datetime.time: _convert_time_param,
    }


def build_asyncpg_profile() -> "DriverParameterProfile":
    """Create the AsyncPG driver parameter profile."""

    return DriverParameterProfile(
        name="AsyncPG",
        default_style=ParameterStyle.NUMERIC,
        supported_styles={ParameterStyle.NUMERIC, ParameterStyle.POSITIONAL_PYFORMAT},
        default_execution_style=ParameterStyle.NUMERIC,
        supported_execution_styles={ParameterStyle.NUMERIC},
        has_native_list_expansion=True,
        preserve_parameter_format=True,
        needs_static_script_compilation=False,
        allow_mixed_parameter_styles=False,
        preserve_original_params_for_many=False,
        json_serializer_strategy="driver",
        custom_type_coercions=_build_asyncpg_custom_type_coercions(),
        default_dialect="postgres",
    )


def configure_asyncpg_parameter_serializers(
    parameter_config: "ParameterStyleConfig",
    serializer: "Callable[[Any], str]",
    *,
    deserializer: "Callable[[str], Any] | None" = None,
) -> "ParameterStyleConfig":
    """Return a parameter configuration updated with AsyncPG JSON codecs."""

    effective_deserializer = deserializer or parameter_config.json_deserializer or from_json
    return parameter_config.replace(json_serializer=serializer, json_deserializer=effective_deserializer)
