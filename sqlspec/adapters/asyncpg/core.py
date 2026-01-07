"""AsyncPG adapter compiled helpers."""

import datetime
import re
from typing import TYPE_CHECKING, Any

from sqlspec.core import DriverParameterProfile, ParameterStyle
from sqlspec.utils.serializers import from_json

if TYPE_CHECKING:
    from collections.abc import Callable

    from sqlspec.core import ParameterStyleConfig

__all__ = ("build_asyncpg_profile", "configure_asyncpg_parameter_serializers", "parse_asyncpg_status")

ASYNC_PG_STATUS_REGEX: "re.Pattern[str]" = re.compile(r"^([A-Z]+)(?:\s+(\d+))?\s+(\d+)$", re.IGNORECASE)
EXPECTED_REGEX_GROUPS = 3


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


def parse_asyncpg_status(status: str) -> int:
    """Parse AsyncPG status string to extract row count.

    AsyncPG returns status strings like "INSERT 0 1", "UPDATE 3", "DELETE 2"
    for non-SELECT operations. This method extracts the affected row count.

    Args:
        status: Status string from AsyncPG operation.

    Returns:
        Number of affected rows, or 0 if cannot parse.
    """
    if not status:
        return 0

    match = ASYNC_PG_STATUS_REGEX.match(status.strip())
    if match:
        groups = match.groups()
        if len(groups) >= EXPECTED_REGEX_GROUPS:
            try:
                return int(groups[-1])
            except (ValueError, IndexError):
                pass

    return 0
