"""psycopg adapter compiled helpers."""

import datetime
from typing import TYPE_CHECKING, Any

import psycopg
from psycopg import sql as psycopg_sql

from sqlspec.core import DriverParameterProfile, ParameterStyle
from sqlspec.exceptions import SQLSpecError

if TYPE_CHECKING:
    from collections.abc import Callable

__all__ = ("build_copy_from_command", "build_psycopg_profile", "build_truncate_command", "psycopg_pipeline_supported")


def psycopg_pipeline_supported() -> bool:
    """Return True when libpq pipeline support is available."""
    try:
        capabilities = psycopg.capabilities
    except AttributeError:
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


def _build_psycopg_custom_type_coercions() -> dict[type, "Callable[[Any], Any]"]:
    """Return custom type coercions for psycopg."""

    return {datetime.datetime: _identity, datetime.date: _identity, datetime.time: _identity}


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
