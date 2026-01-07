"""psycopg adapter compiled helpers."""

import datetime
from typing import TYPE_CHECKING, Any

from psycopg import sql as psycopg_sql

from sqlspec.core import DriverParameterProfile, ParameterStyle, StatementConfig, build_statement_config_from_profile
from sqlspec.exceptions import SQLSpecError
from sqlspec.typing import PGVECTOR_INSTALLED
from sqlspec.utils.serializers import to_json
from sqlspec.utils.type_converters import build_json_list_converter, build_json_tuple_converter

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping

    from sqlspec.core import ParameterStyleConfig

__all__ = (
    "apply_psycopg_driver_features",
    "build_copy_from_command",
    "build_psycopg_parameter_config",
    "build_psycopg_profile",
    "build_psycopg_statement_config",
    "build_truncate_command",
    "psycopg_pipeline_supported",
    "psycopg_statement_config",
)


def psycopg_pipeline_supported() -> bool:
    """Return True when libpq pipeline support is available."""
    try:
        import psycopg

        capabilities = psycopg.capabilities
    except (ImportError, AttributeError):
        return False
    try:
        return bool(capabilities.has_pipeline())
    except Exception:
        return False


def _compose_table_identifier(table: str) -> "psycopg_sql.Composed":
    parts = [part for part in table.split(".") if part]
    if not parts:
        msg = "Table name must not be empty"
        raise SQLSpecError(msg)
    identifiers = [psycopg_sql.Identifier(part) for part in parts]
    return psycopg_sql.SQL(".").join(identifiers)


def build_copy_from_command(table: str, columns: "list[str]") -> "psycopg_sql.Composed":
    table_identifier = _compose_table_identifier(table)
    column_sql = psycopg_sql.SQL(", ").join([psycopg_sql.Identifier(column) for column in columns])
    return psycopg_sql.SQL("COPY {} ({}) FROM STDIN").format(table_identifier, column_sql)


def build_truncate_command(table: str) -> "psycopg_sql.Composed":
    return psycopg_sql.SQL("TRUNCATE TABLE {}").format(_compose_table_identifier(table))


def _identity(value: Any) -> Any:
    return value


def _build_psycopg_custom_type_coercions() -> "dict[type, Callable[[Any], Any]]":
    """Return custom type coercions for psycopg."""

    return {datetime.datetime: _identity, datetime.date: _identity, datetime.time: _identity}


def build_psycopg_parameter_config(
    profile: "DriverParameterProfile", serializer: "Callable[[Any], str]"
) -> "ParameterStyleConfig":
    """Construct parameter configuration with shared JSON serializer support.

    Args:
        profile: Driver parameter profile to extend.
        serializer: JSON serializer for parameter coercion.

    Returns:
        ParameterStyleConfig with updated type coercions.
    """

    base_config = build_statement_config_from_profile(profile, json_serializer=serializer).parameter_config

    updated_type_map = dict(base_config.type_coercion_map)
    updated_type_map[list] = build_json_list_converter(serializer)
    updated_type_map[tuple] = build_json_tuple_converter(serializer)

    return base_config.replace(type_coercion_map=updated_type_map)


def build_psycopg_profile() -> "DriverParameterProfile":
    """Create the psycopg driver parameter profile."""

    return DriverParameterProfile(
        name="Psycopg",
        default_style=ParameterStyle.POSITIONAL_PYFORMAT,
        supported_styles={
            ParameterStyle.POSITIONAL_PYFORMAT,
            ParameterStyle.NAMED_PYFORMAT,
            ParameterStyle.NUMERIC,
            ParameterStyle.QMARK,
        },
        default_execution_style=ParameterStyle.POSITIONAL_PYFORMAT,
        supported_execution_styles={ParameterStyle.POSITIONAL_PYFORMAT, ParameterStyle.NAMED_PYFORMAT},
        has_native_list_expansion=True,
        preserve_parameter_format=True,
        needs_static_script_compilation=False,
        allow_mixed_parameter_styles=False,
        preserve_original_params_for_many=False,
        json_serializer_strategy="helper",
        custom_type_coercions=_build_psycopg_custom_type_coercions(),
        default_dialect="postgres",
    )


def build_psycopg_statement_config(*, json_serializer: "Callable[[Any], str] | None" = None) -> "StatementConfig":
    """Construct the psycopg statement configuration with optional JSON codecs."""
    serializer = json_serializer or to_json
    parameter_config = build_psycopg_parameter_config(build_psycopg_profile(), serializer)
    base_config = build_statement_config_from_profile(build_psycopg_profile(), json_serializer=serializer)
    return base_config.replace(parameter_config=parameter_config)


psycopg_statement_config = build_psycopg_statement_config()


def apply_psycopg_driver_features(
    statement_config: "StatementConfig", driver_features: "Mapping[str, Any] | None"
) -> "tuple[StatementConfig, dict[str, Any]]":
    """Apply psycopg driver feature defaults to statement config."""
    processed_driver_features: dict[str, Any] = dict(driver_features) if driver_features else {}
    serializer = processed_driver_features.get("json_serializer", to_json)
    processed_driver_features.setdefault("json_serializer", serializer)
    processed_driver_features.setdefault("enable_pgvector", PGVECTOR_INSTALLED)

    parameter_config = build_psycopg_parameter_config(build_psycopg_profile(), serializer)
    statement_config = statement_config.replace(parameter_config=parameter_config)

    return statement_config, processed_driver_features
