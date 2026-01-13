"""OracleDB adapter compiled helpers."""

import re
from collections.abc import Sized
from typing import TYPE_CHECKING, Any

from sqlspec.adapters.oracledb.type_converter import OracleOutputConverter
from sqlspec.core import (
    DriverParameterProfile,
    ParameterStyle,
    StackResult,
    StatementConfig,
    build_statement_config_from_profile,
    create_sql_result,
)
from sqlspec.exceptions import (
    CheckViolationError,
    DatabaseConnectionError,
    DataError,
    ForeignKeyViolationError,
    IntegrityError,
    NotNullViolationError,
    OperationalError,
    SQLParsingError,
    SQLSpecError,
    TransactionError,
    UniqueViolationError,
)
from sqlspec.typing import NUMPY_INSTALLED
from sqlspec.utils.serializers import to_json
from sqlspec.utils.type_guards import has_rowcount, is_readable

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping

    from sqlspec.core import SQL

__all__ = (
    "apply_driver_features",
    "build_insert_statement",
    "build_pipeline_stack_result",
    "build_profile",
    "build_statement_config",
    "build_truncate_statement",
    "coerce_large_string_parameters_async",
    "coerce_large_string_parameters_sync",
    "collect_async_rows",
    "collect_sync_rows",
    "create_mapped_exception",
    "default_statement_config",
    "driver_profile",
    "normalize_column_names",
    "normalize_execute_many_parameters_async",
    "normalize_execute_many_parameters_sync",
    "requires_session_callback",
    "resolve_rowcount",
)


IMPLICIT_UPPER_COLUMN_PATTERN: "re.Pattern[str]" = re.compile(r"^(?!\d)(?:[A-Z0-9_]+)$")
_VERSION_COMPONENTS: int = 3
TYPE_CONVERTER = OracleOutputConverter()

ORA_CHECK_CONSTRAINT = 2290
ORA_INTEGRITY_RANGE_START = 2200
ORA_INTEGRITY_RANGE_END = 2300
ORA_PARSING_RANGE_START = 900
ORA_PARSING_RANGE_END = 1000
ORA_TABLESPACE_FULL = 1652

_ERROR_CODE_MAPPING: "dict[int, tuple[type[SQLSpecError], str]]" = {
    1: (UniqueViolationError, "unique constraint violation"),
    2291: (ForeignKeyViolationError, "foreign key constraint violation"),
    2292: (ForeignKeyViolationError, "foreign key constraint violation"),
    ORA_CHECK_CONSTRAINT: (CheckViolationError, "check constraint violation"),
    1400: (NotNullViolationError, "not-null constraint violation"),
    1407: (NotNullViolationError, "not-null constraint violation"),
    1017: (DatabaseConnectionError, "connection error"),
    12154: (DatabaseConnectionError, "connection error"),
    12541: (DatabaseConnectionError, "connection error"),
    12545: (DatabaseConnectionError, "connection error"),
    12514: (DatabaseConnectionError, "connection error"),
    12505: (DatabaseConnectionError, "connection error"),
    60: (TransactionError, "transaction error"),
    8176: (TransactionError, "transaction error"),
    1722: (DataError, "data error"),
    1858: (DataError, "data error"),
    1840: (DataError, "data error"),
    ORA_TABLESPACE_FULL: (OperationalError, "operational error"),
}


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


def normalize_execute_many_parameters_sync(parameters: Any) -> Any:
    """Normalize parameters for Oracle executemany calls.

    Args:
        parameters: Prepared parameters payload.

    Returns:
        Normalized parameters payload.

    Raises:
        ValueError: When parameters are missing for executemany.
    """
    if not parameters:
        msg = "execute_many requires parameters"
        raise ValueError(msg)
    if isinstance(parameters, tuple):
        return list(parameters)
    return parameters


def normalize_execute_many_parameters_async(parameters: Any) -> Any:
    """Normalize parameters for Oracle async executemany calls.

    Args:
        parameters: Prepared parameters payload.

    Returns:
        Normalized parameters payload.

    Raises:
        ValueError: When parameters are missing for executemany.
    """
    if not parameters:
        msg = "execute_many requires parameters"
        raise ValueError(msg)
    return parameters


def coerce_large_string_parameters_sync(connection: Any, parameters: Any, *, lob_type: Any, threshold: int) -> Any:
    """Coerce large string parameters into CLOBs.

    Args:
        connection: Oracle database connection.
        parameters: Prepared parameters payload.
        lob_type: Oracle CLOB type.
        threshold: String length threshold for CLOB conversion.

    Returns:
        Parameters payload with large strings converted to CLOBs.
    """
    if not parameters or not isinstance(parameters, dict):
        return parameters
    for param_name, param_value in parameters.items():
        if isinstance(param_value, str) and len(param_value) > threshold:
            clob = connection.createlob(lob_type)
            clob.write(param_value)
            parameters[param_name] = clob
    return parameters


async def coerce_large_string_parameters_async(
    connection: Any, parameters: Any, *, lob_type: Any, threshold: int
) -> Any:
    """Coerce large string parameters into CLOBs for async Oracle drivers.

    Args:
        connection: Oracle database connection.
        parameters: Prepared parameters payload.
        lob_type: Oracle CLOB type.
        threshold: String length threshold for CLOB conversion.

    Returns:
        Parameters payload with large strings converted to CLOBs.
    """
    if not parameters or not isinstance(parameters, dict):
        return parameters
    for param_name, param_value in parameters.items():
        if isinstance(param_value, str) and len(param_value) > threshold:
            clob = await connection.createlob(lob_type)
            await clob.write(param_value)
            parameters[param_name] = clob
    return parameters


def build_insert_statement(table: str, columns: "list[str]") -> str:
    column_list = ", ".join(columns)
    placeholders = ", ".join(f":{idx + 1}" for idx in range(len(columns)))
    return f"INSERT INTO {table} ({column_list}) VALUES ({placeholders})"


def build_truncate_statement(table: str) -> str:
    return f"TRUNCATE TABLE {table}"


def build_pipeline_stack_result(
    statement: "SQL",
    method: str,
    returns_rows: bool,
    parameters: Any,
    pipeline_result: Any,
    driver_features: "dict[str, Any]",
) -> "StackResult":
    """Build StackResult from Oracle pipeline output.

    Args:
        statement: Statement executed in the pipeline.
        method: Pipeline execution method name.
        returns_rows: Whether the operation returns rows.
        parameters: Prepared parameters used for execution.
        pipeline_result: Raw pipeline execution result.
        driver_features: Driver feature configuration for normalization.

    Returns:
        StackResult for the pipeline operation.
    """
    try:
        rows = pipeline_result.rows
    except AttributeError:
        rows = None
    try:
        columns = pipeline_result.columns
    except AttributeError:
        columns = None

    data: list[dict[str, Any]] | None = None
    if returns_rows:
        if not rows:
            data = []
        else:
            if columns:
                names = []
                for index, column in enumerate(columns):
                    try:
                        name = column.name
                    except AttributeError:
                        name = f"column_{index}"
                    names.append(name)
            else:
                first = rows[0]
                names = [f"column_{index}" for index in range(len(first) if isinstance(first, Sized) else 0)]
            names = normalize_column_names(names, driver_features)
            normalized_rows: list[dict[str, Any]] = []
            for row in rows:
                if isinstance(row, dict):
                    normalized_rows.append(row)
                else:
                    normalized_rows.append(dict(zip(names, row, strict=False)))
            data = normalized_rows

    metadata: dict[str, Any] = {"pipeline_operation": method}
    try:
        warning = pipeline_result.warning
    except AttributeError:
        warning = None
    if warning is not None:
        metadata["warning"] = warning

    try:
        return_value = pipeline_result.return_value
    except AttributeError:
        return_value = None
    if return_value is not None:
        metadata["return_value"] = return_value

    try:
        rowcount = pipeline_result.rowcount
    except AttributeError:
        rowcount = None

    if isinstance(rowcount, int) and rowcount >= 0:
        rows_affected = rowcount
    elif method == "execute_many":
        try:
            rows_affected = len(parameters or ())
        except TypeError:
            rows_affected = 0
    elif method == "execute" and not returns_rows:
        rows_affected = 1
    elif returns_rows:
        rows_affected = len(data or [])
    else:
        rows_affected = 0

    sql_result = create_sql_result(statement, data=data, rows_affected=rows_affected, metadata=metadata)
    return StackResult.from_sql_result(sql_result)


def resolve_rowcount(cursor: Any) -> int:
    """Resolve rowcount from an Oracle cursor.

    Args:
        cursor: Oracle cursor with optional rowcount metadata.

    Returns:
        Rowcount value or 0 when unavailable.
    """
    if not has_rowcount(cursor):
        return 0
    rowcount = cursor.rowcount
    if isinstance(rowcount, int):
        return rowcount
    return 0


def apply_driver_features(driver_features: "Mapping[str, Any] | None") -> "dict[str, Any]":
    """Apply OracleDB driver feature defaults."""
    features: dict[str, Any] = dict(driver_features) if driver_features else {}
    features.setdefault("enable_numpy_vectors", NUMPY_INSTALLED)
    features.setdefault("enable_lowercase_column_names", True)
    features.setdefault("enable_uuid_binary", True)
    return features


def requires_session_callback(driver_features: "dict[str, Any]") -> bool:
    """Return True when the session callback should be installed."""
    enable_numpy_vectors = bool(driver_features.get("enable_numpy_vectors", False))
    enable_uuid_binary = bool(driver_features.get("enable_uuid_binary", False))
    return enable_numpy_vectors or enable_uuid_binary


def _coerce_sync_row_values(row: "tuple[Any, ...]") -> "list[Any]":
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


async def _coerce_async_row_values(row: "tuple[Any, ...]") -> "list[Any]":
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


def collect_sync_rows(
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
    data = [dict(zip(column_names, _coerce_sync_row_values(row), strict=False)) for row in fetched_data]
    return data, column_names


async def collect_async_rows(
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
        coerced_row = await _coerce_async_row_values(row)
        data.append(dict(zip(column_names, coerced_row, strict=False)))
    return data, column_names


def _create_oracle_error(
    error: Any, code: "int | None", error_class: type[SQLSpecError], description: str
) -> SQLSpecError:
    """Create a SQLSpec exception from an Oracle error.

    Args:
        error: The original Oracle exception
        code: Oracle error code
        error_class: The SQLSpec exception class to instantiate
        description: Human-readable description of the error type

    Returns:
        A new SQLSpec exception instance with the original as its cause
    """
    msg = f"Oracle {description} [ORA-{code:05d}]: {error}" if code else f"Oracle {description}: {error}"
    exc = error_class(msg)
    exc.__cause__ = error
    return exc


def create_mapped_exception(error: Any) -> SQLSpecError:
    """Map Oracle exceptions to SQLSpec exceptions.

    This is a factory function that returns an exception instance rather than
    raising. This pattern is more robust for use in __exit__ handlers and
    avoids issues with exception control flow in different Python versions.

    Args:
        error: The Oracle exception to map

    Returns:
        A SQLSpec exception that wraps the original error
    """
    error_obj = error.args[0] if getattr(error, "args", None) else None
    if not error_obj:
        return _create_oracle_error(error, None, SQLSpecError, "database error")

    try:
        error_code = error_obj.code
    except AttributeError:
        error_code = None
    if not error_code:
        return _create_oracle_error(error, None, SQLSpecError, "database error")

    mapping = _ERROR_CODE_MAPPING.get(error_code)
    if mapping:
        error_class, error_desc = mapping
        return _create_oracle_error(error, error_code, error_class, error_desc)

    if ORA_INTEGRITY_RANGE_START <= error_code < ORA_INTEGRITY_RANGE_END:
        return _create_oracle_error(error, error_code, IntegrityError, "integrity constraint violation")

    if ORA_PARSING_RANGE_START <= error_code < ORA_PARSING_RANGE_END:
        return _create_oracle_error(error, error_code, SQLParsingError, "SQL syntax error")

    return _create_oracle_error(error, error_code, SQLSpecError, "database error")


def build_profile() -> "DriverParameterProfile":
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


driver_profile = build_profile()


def build_statement_config(*, json_serializer: "Callable[[Any], str] | None" = None) -> StatementConfig:
    """Construct the OracleDB statement configuration with optional JSON serializer."""
    serializer = json_serializer or to_json
    profile = driver_profile
    return build_statement_config_from_profile(
        profile, statement_overrides={"dialect": "oracle"}, json_serializer=serializer
    )


default_statement_config = build_statement_config()
