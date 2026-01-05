"""AsyncMy adapter compiled helpers."""

from sqlspec.core import DriverParameterProfile, ParameterStyle
from sqlspec.exceptions import SQLSpecError


def _bool_to_int(value: bool) -> int:
    return int(value)


def _quote_mysql_identifier(identifier: str) -> str:
    normalized = identifier.replace("`", "``")
    return f"`{normalized}`"


def _format_mysql_identifier(identifier: str) -> str:
    cleaned = identifier.strip()
    if not cleaned:
        msg = "Table name must not be empty"
        raise SQLSpecError(msg)
    parts = [part for part in cleaned.split(".") if part]
    formatted = ".".join(_quote_mysql_identifier(part) for part in parts)
    return formatted or _quote_mysql_identifier(cleaned)


def _build_asyncmy_insert_statement(table: str, columns: "list[str]") -> str:
    column_clause = ", ".join(_quote_mysql_identifier(column) for column in columns)
    placeholders = ", ".join("%s" for _ in columns)
    return f"INSERT INTO {_format_mysql_identifier(table)} ({column_clause}) VALUES ({placeholders})"


def _build_asyncmy_profile() -> "DriverParameterProfile":
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
