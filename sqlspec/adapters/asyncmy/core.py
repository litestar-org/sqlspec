"""AsyncMy adapter compiled helpers."""

from typing import TYPE_CHECKING, Any

from sqlspec.core import DriverParameterProfile, ParameterStyle, StatementConfig, build_statement_config_from_profile
from sqlspec.exceptions import SQLSpecError
from sqlspec.utils.serializers import from_json, to_json
from sqlspec.utils.type_guards import has_cursor_metadata, has_type_code

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping

__all__ = (
    "apply_asyncmy_driver_features",
    "asyncmy_statement_config",
    "build_asyncmy_insert_statement",
    "build_asyncmy_profile",
    "build_asyncmy_statement_config",
    "deserialize_asyncmy_json_rows",
    "detect_asyncmy_json_columns",
    "format_mysql_identifier",
)


def _bool_to_int(value: bool) -> int:
    return int(value)


def _quote_mysql_identifier(identifier: str) -> str:
    normalized = identifier.replace("`", "``")
    return f"`{normalized}`"


def format_mysql_identifier(identifier: str) -> str:
    cleaned = identifier.strip()
    if not cleaned:
        msg = "Table name must not be empty"
        raise SQLSpecError(msg)
    parts = [part for part in cleaned.split(".") if part]
    formatted = ".".join(_quote_mysql_identifier(part) for part in parts)
    return formatted or _quote_mysql_identifier(cleaned)


def build_asyncmy_insert_statement(table: str, columns: "list[str]") -> str:
    column_clause = ", ".join(_quote_mysql_identifier(column) for column in columns)
    placeholders = ", ".join("%s" for _ in columns)
    return f"INSERT INTO {format_mysql_identifier(table)} ({column_clause}) VALUES ({placeholders})"


def build_asyncmy_profile() -> "DriverParameterProfile":
    """Create the AsyncMy driver parameter profile."""

    return DriverParameterProfile(
        name="AsyncMy",
        default_style=ParameterStyle.QMARK,
        supported_styles={ParameterStyle.QMARK, ParameterStyle.POSITIONAL_PYFORMAT},
        default_execution_style=ParameterStyle.POSITIONAL_PYFORMAT,
        supported_execution_styles={ParameterStyle.POSITIONAL_PYFORMAT},
        has_native_list_expansion=False,
        preserve_parameter_format=True,
        needs_static_script_compilation=True,
        allow_mixed_parameter_styles=False,
        preserve_original_params_for_many=False,
        json_serializer_strategy="helper",
        custom_type_coercions={bool: _bool_to_int},
        default_dialect="mysql",
    )


def build_asyncmy_statement_config(
    *, json_serializer: "Callable[[Any], str] | None" = None, json_deserializer: "Callable[[str], Any] | None" = None
) -> "StatementConfig":
    """Construct the AsyncMy statement configuration with optional JSON codecs."""
    serializer = json_serializer or to_json
    deserializer = json_deserializer or from_json
    return build_statement_config_from_profile(
        build_asyncmy_profile(),
        statement_overrides={"dialect": "mysql"},
        json_serializer=serializer,
        json_deserializer=deserializer,
    )


asyncmy_statement_config = build_asyncmy_statement_config()


def apply_asyncmy_driver_features(
    statement_config: "StatementConfig", driver_features: "Mapping[str, Any] | None"
) -> "tuple[StatementConfig, dict[str, Any]]":
    """Apply AsyncMy driver feature defaults to statement config."""
    processed_driver_features: dict[str, Any] = dict(driver_features) if driver_features else {}
    json_serializer = processed_driver_features.setdefault("json_serializer", to_json)
    json_deserializer = processed_driver_features.setdefault("json_deserializer", from_json)

    if json_serializer is not None:
        parameter_config = statement_config.parameter_config.with_json_serializers(
            json_serializer, deserializer=json_deserializer
        )
        statement_config = statement_config.replace(parameter_config=parameter_config)

    return statement_config, processed_driver_features


def detect_asyncmy_json_columns(cursor: Any, json_type_codes: "set[int]") -> "list[int]":
    """Identify JSON column indexes from cursor metadata.

    Args:
        cursor: Database cursor with description metadata available.
        json_type_codes: Set of type codes identifying JSON columns.

    Returns:
        List of index positions where JSON values are present.
    """
    if not has_cursor_metadata(cursor):
        return []
    description = cursor.description
    if not description or not json_type_codes:
        return []

    json_indexes: list[int] = []
    for index, column in enumerate(description):
        if has_type_code(column):
            type_code = column.type_code
        elif isinstance(column, (tuple, list)) and len(column) > 1:
            type_code = column[1]
        else:
            type_code = None
        if type_code in json_type_codes:
            json_indexes.append(index)
    return json_indexes


def deserialize_asyncmy_json_rows(
    column_names: "list[str]",
    rows: "list[dict[str, Any]]",
    json_indexes: "list[int]",
    deserializer: "Callable[[Any], Any]",
    *,
    logger: Any | None = None,
) -> "list[dict[str, Any]]":
    """Apply JSON deserialization to selected columns.

    Args:
        column_names: Ordered column names from the cursor description.
        rows: Result rows represented as dictionaries.
        json_indexes: Column indexes to deserialize.
        deserializer: Callable used to decode JSON values.
        logger: Optional logger for debug output.

    Returns:
        Rows with JSON columns decoded when possible.
    """
    if not rows or not column_names or not json_indexes:
        return rows

    target_columns = [column_names[index] for index in json_indexes if index < len(column_names)]
    if not target_columns:
        return rows

    for row in rows:
        for column in target_columns:
            if column not in row:
                continue
            raw_value = row[column]
            if raw_value is None:
                continue
            if isinstance(raw_value, bytearray):
                raw_value = bytes(raw_value)
            if not isinstance(raw_value, (str, bytes)):
                continue
            try:
                row[column] = deserializer(raw_value)
            except Exception:
                if logger is not None:
                    logger.debug("Failed to deserialize JSON column %s", column, exc_info=True)
    return rows
