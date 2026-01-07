"""SQLite adapter compiled helpers."""

from datetime import date, datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from sqlspec.core import DriverParameterProfile, ParameterStyle, StatementConfig, build_statement_config_from_profile
from sqlspec.exceptions import SQLSpecError
from sqlspec.utils.serializers import from_json, to_json
from sqlspec.utils.type_converters import build_decimal_converter, build_time_iso_converter

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence

__all__ = (
    "apply_sqlite_driver_features",
    "build_sqlite_profile",
    "build_sqlite_statement_config",
    "format_sqlite_identifier",
    "process_sqlite_result",
    "sqlite_statement_config",
)


_TIME_TO_ISO = build_time_iso_converter()
_DECIMAL_TO_STRING = build_decimal_converter(mode="string")


def _bool_to_int(value: bool) -> int:
    return int(value)


def _quote_sqlite_identifier(identifier: str) -> str:
    normalized = identifier.replace('"', '""')
    return f'"{normalized}"'


def format_sqlite_identifier(identifier: str) -> str:
    cleaned = identifier.strip()
    if not cleaned:
        msg = "Table name must not be empty"
        raise SQLSpecError(msg)

    if "." not in cleaned:
        return _quote_sqlite_identifier(cleaned)

    return ".".join(_quote_sqlite_identifier(part) for part in cleaned.split(".") if part)


def build_sqlite_insert_statement(table: str, columns: "list[str]") -> str:
    column_clause = ", ".join(_quote_sqlite_identifier(column) for column in columns)
    placeholders = ", ".join("?" for _ in columns)
    return f"INSERT INTO {format_sqlite_identifier(table)} ({column_clause}) VALUES ({placeholders})"


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


def build_sqlite_profile() -> "DriverParameterProfile":
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


def build_sqlite_statement_config(
    *, json_serializer: "Callable[[Any], str] | None" = None, json_deserializer: "Callable[[str], Any] | None" = None
) -> "StatementConfig":
    """Construct the SQLite statement configuration with optional JSON codecs."""
    serializer = json_serializer or to_json
    deserializer = json_deserializer or from_json
    return build_statement_config_from_profile(
        build_sqlite_profile(),
        statement_overrides={"dialect": "sqlite"},
        json_serializer=serializer,
        json_deserializer=deserializer,
    )


sqlite_statement_config = build_sqlite_statement_config()


def apply_sqlite_driver_features(
    statement_config: "StatementConfig", driver_features: "dict[str, Any] | None"
) -> "tuple[StatementConfig, dict[str, Any]]":
    """Apply SQLite driver feature defaults to statement config."""
    processed_driver_features: dict[str, Any] = dict(driver_features) if driver_features else {}
    processed_driver_features.setdefault("enable_custom_adapters", True)
    json_serializer = processed_driver_features.setdefault("json_serializer", to_json)
    json_deserializer = processed_driver_features.setdefault("json_deserializer", from_json)

    if json_serializer is not None:
        parameter_config = statement_config.parameter_config.with_json_serializers(
            json_serializer, deserializer=json_deserializer
        )
        statement_config = statement_config.replace(parameter_config=parameter_config)

    return statement_config, processed_driver_features
