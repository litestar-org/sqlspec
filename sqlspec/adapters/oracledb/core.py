"""OracleDB adapter compiled helpers."""

import re
from typing import TYPE_CHECKING, Any

from sqlspec.adapters.oracledb.type_converter import OracleOutputConverter
from sqlspec.core import DriverParameterProfile, ParameterStyle, StatementConfig, build_statement_config_from_profile
from sqlspec.typing import NUMPY_INSTALLED
from sqlspec.utils.serializers import to_json
from sqlspec.utils.type_guards import is_readable

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping

__all__ = (
    "apply_oracledb_driver_features",
    "build_oracledb_profile",
    "build_oracledb_statement_config",
    "coerce_async_row_values",
    "coerce_sync_row_values",
    "collect_oracledb_async_rows",
    "collect_oracledb_sync_rows",
    "normalize_column_names",
    "oracle_insert_statement",
    "oracle_truncate_statement",
    "oracledb_statement_config",
    "requires_oracledb_session_callback",
)


IMPLICIT_UPPER_COLUMN_PATTERN: "re.Pattern[str]" = re.compile(r"^(?!\d)(?:[A-Z0-9_]+)$")
_VERSION_COMPONENTS: int = 3
TYPE_CONVERTER = OracleOutputConverter()


def _parse_version_tuple(version: str) -> "tuple[int, int, int]":
    parts = [int(part) for part in version.split(".") if part.isdigit()]
    while len(parts) < _VERSION_COMPONENTS:
        parts.append(0)
    return parts[0], parts[1], parts[2]


def _resolve_oracledb_version() -> "tuple[int, int, int]":
    try:
        import oracledb
    except ImportError:
        return (0, 0, 0)
    try:
        version = oracledb.__version__
    except AttributeError:
        version = "0.0.0"
    return _parse_version_tuple(version)


ORACLEDB_VERSION: "tuple[int, int, int]" = _resolve_oracledb_version()


def normalize_column_names(column_names: "list[str]", driver_features: "dict[str, Any]") -> "list[str]":
    should_lowercase = driver_features.get("enable_lowercase_column_names", False)
    if not should_lowercase:
        return column_names
    normalized: list[str] = []
    for name in column_names:
        if name and IMPLICIT_UPPER_COLUMN_PATTERN.fullmatch(name):
            normalized.append(name.lower())
        else:
            normalized.append(name)
    return normalized


def oracle_insert_statement(table: str, columns: "list[str]") -> str:
    column_list = ", ".join(columns)
    placeholders = ", ".join(f":{idx + 1}" for idx in range(len(columns)))
    return f"INSERT INTO {table} ({column_list}) VALUES ({placeholders})"


def oracle_truncate_statement(table: str) -> str:
    return f"TRUNCATE TABLE {table}"


def apply_oracledb_driver_features(driver_features: "Mapping[str, Any] | None") -> "dict[str, Any]":
    """Apply OracleDB driver feature defaults."""
    processed_driver_features: dict[str, Any] = dict(driver_features) if driver_features else {}
    processed_driver_features.setdefault("enable_numpy_vectors", NUMPY_INSTALLED)
    processed_driver_features.setdefault("enable_lowercase_column_names", True)
    processed_driver_features.setdefault("enable_uuid_binary", True)
    return processed_driver_features


def requires_oracledb_session_callback(driver_features: "dict[str, Any]") -> bool:
    """Return True when the session callback should be installed."""
    enable_numpy_vectors = bool(driver_features.get("enable_numpy_vectors", False))
    enable_uuid_binary = bool(driver_features.get("enable_uuid_binary", False))
    return enable_numpy_vectors or enable_uuid_binary


def coerce_sync_row_values(row: "tuple[Any, ...]") -> "list[Any]":
    """Coerce LOB handles to concrete values for synchronous execution.

    Processes each value in the row, reading LOB objects and applying
    type detection for JSON values stored in CLOBs.

    Args:
        row: Tuple of column values from database fetch.

    Returns:
        List of coerced values with LOBs read to strings/bytes.

    """
    coerced_values: list[Any] = []
    for value in row:
        if is_readable(value):
            try:
                processed_value = value.read()
            except Exception:
                coerced_values.append(value)
                continue
            if isinstance(processed_value, str):
                processed_value = TYPE_CONVERTER.convert_if_detected(processed_value)
            coerced_values.append(processed_value)
            continue
        coerced_values.append(value)
    return coerced_values


async def coerce_async_row_values(row: "tuple[Any, ...]") -> "list[Any]":
    """Coerce LOB handles to concrete values for asynchronous execution.

    Processes each value in the row, reading LOB objects asynchronously
    and applying type detection for JSON values stored in CLOBs.

    Args:
        row: Tuple of column values from database fetch.

    Returns:
        List of coerced values with LOBs read to strings/bytes.

    """
    coerced_values: list[Any] = []
    for value in row:
        if is_readable(value):
            try:
                processed_value = await TYPE_CONVERTER.process_lob(value)
            except Exception:
                coerced_values.append(value)
                continue
            if isinstance(processed_value, str):
                processed_value = TYPE_CONVERTER.convert_if_detected(processed_value)
            coerced_values.append(processed_value)
        else:
            coerced_values.append(value)
    return coerced_values


def collect_oracledb_sync_rows(
    fetched_data: "list[Any] | None", description: "list[Any] | None", driver_features: "dict[str, Any]"
) -> "tuple[list[dict[str, Any]], list[str]]":
    """Collect OracleDB sync rows into dictionaries with normalized column names.

    Args:
        fetched_data: Rows returned from cursor.fetchall().
        description: Cursor description metadata.
        driver_features: Driver feature configuration.

    Returns:
        Tuple of (rows, column_names).
    """
    if not description:
        return [], []
    column_names = [col[0] for col in description]
    column_names = normalize_column_names(column_names, driver_features)
    if not fetched_data:
        return [], column_names
    data = [dict(zip(column_names, coerce_sync_row_values(row), strict=False)) for row in fetched_data]
    return data, column_names


async def collect_oracledb_async_rows(
    fetched_data: "list[Any] | None", description: "list[Any] | None", driver_features: "dict[str, Any]"
) -> "tuple[list[dict[str, Any]], list[str]]":
    """Collect OracleDB async rows into dictionaries with normalized column names.

    Args:
        fetched_data: Rows returned from cursor.fetchall().
        description: Cursor description metadata.
        driver_features: Driver feature configuration.

    Returns:
        Tuple of (rows, column_names).
    """
    if not description:
        return [], []
    column_names = [col[0] for col in description]
    column_names = normalize_column_names(column_names, driver_features)
    if not fetched_data:
        return [], column_names
    data: list[dict[str, Any]] = []
    for row in fetched_data:
        coerced_row = await coerce_async_row_values(row)
        data.append(dict(zip(column_names, coerced_row, strict=False)))
    return data, column_names


def build_oracledb_profile() -> "DriverParameterProfile":
    """Create the OracleDB driver parameter profile."""
    return DriverParameterProfile(
        name="OracleDB",
        default_style=ParameterStyle.POSITIONAL_COLON,
        supported_styles={ParameterStyle.NAMED_COLON, ParameterStyle.POSITIONAL_COLON, ParameterStyle.QMARK},
        default_execution_style=ParameterStyle.NAMED_COLON,
        supported_execution_styles={ParameterStyle.NAMED_COLON, ParameterStyle.POSITIONAL_COLON},
        has_native_list_expansion=False,
        preserve_parameter_format=True,
        needs_static_script_compilation=False,
        allow_mixed_parameter_styles=False,
        preserve_original_params_for_many=False,
        json_serializer_strategy="helper",
        default_dialect="oracle",
    )


def build_oracledb_statement_config(*, json_serializer: "Callable[[Any], str] | None" = None) -> StatementConfig:
    """Construct the OracleDB statement configuration with optional JSON serializer."""
    serializer = json_serializer or to_json
    profile = build_oracledb_profile()
    return build_statement_config_from_profile(
        profile, statement_overrides={"dialect": "oracle"}, json_serializer=serializer
    )


oracledb_statement_config = build_oracledb_statement_config()
