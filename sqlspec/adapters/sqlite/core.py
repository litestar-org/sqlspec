"""SQLite adapter compiled helpers."""

from datetime import date, datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from sqlspec.core import DriverParameterProfile, ParameterStyle
from sqlspec.exceptions import SQLSpecError
from sqlspec.utils.type_converters import build_decimal_converter, build_time_iso_converter

if TYPE_CHECKING:
    from collections.abc import Sequence

__all__ = ("process_sqlite_result",)


_TIME_TO_ISO = build_time_iso_converter()
_DECIMAL_TO_STRING = build_decimal_converter(mode="string")


def _bool_to_int(value: bool) -> int:
    return int(value)


def _quote_sqlite_identifier(identifier: str) -> str:
    normalized = identifier.replace('"', '""')
    return f'"{normalized}"'


def _format_sqlite_identifier(identifier: str) -> str:
    cleaned = identifier.strip()
    if not cleaned:
        msg = "Table name must not be empty"
        raise SQLSpecError(msg)

    if "." not in cleaned:
        return _quote_sqlite_identifier(cleaned)

    return ".".join(_quote_sqlite_identifier(part) for part in cleaned.split(".") if part)


def _build_sqlite_insert_statement(table: str, columns: "list[str]") -> str:
    column_clause = ", ".join(_quote_sqlite_identifier(column) for column in columns)
    placeholders = ", ".join("?" for _ in columns)
    return f"INSERT INTO {_format_sqlite_identifier(table)} ({column_clause}) VALUES ({placeholders})"


def process_sqlite_result(
    fetched_data: "list[Any]", description: "Sequence[Any] | None"
) -> "tuple[list[dict[str, Any]], list[str], int]":
    """Process SQLite result rows into dictionaries.

    Optimized helper to convert raw rows and cursor description into list of dicts.

    Args:
        fetched_data: Raw rows from cursor.fetchall()
        description: Cursor description (tuple of tuples)

    Returns:
        Tuple of (data, column_names, row_count)
    """
    if not description:
        return [], [], 0

    column_names = [col[0] for col in description]
    # compiled list comp and zip is faster in mypyc
    data = [dict(zip(column_names, row, strict=False)) for row in fetched_data]
    return data, column_names, len(data)


def _build_sqlite_profile() -> "DriverParameterProfile":
    """Create the SQLite driver parameter profile."""

    return DriverParameterProfile(
        name="SQLite",
        default_style=ParameterStyle.QMARK,
        supported_styles={ParameterStyle.QMARK, ParameterStyle.NAMED_COLON},
        default_execution_style=ParameterStyle.QMARK,
        supported_execution_styles={ParameterStyle.QMARK, ParameterStyle.NAMED_COLON},
        has_native_list_expansion=False,
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
        default_dialect="sqlite",
    )
