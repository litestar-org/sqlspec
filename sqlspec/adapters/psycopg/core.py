"""psycopg adapter compiled helpers."""

import datetime
from typing import TYPE_CHECKING, Any, NamedTuple, cast

from psycopg import sql as psycopg_sql

from sqlspec.core import (
    SQL,
    DriverParameterProfile,
    ParameterStyle,
    StatementConfig,
    build_statement_config_from_profile,
)
from sqlspec.driver import ExecutionResult
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
from sqlspec.typing import PGVECTOR_INSTALLED
from sqlspec.utils.serializers import to_json
from sqlspec.utils.type_converters import build_json_list_converter, build_json_tuple_converter
from sqlspec.utils.type_guards import has_rowcount, has_sqlstate

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping

    from sqlspec.core import ParameterStyleConfig, StackOperation

__all__ = (
    "PipelineCursorEntry",
    "PreparedStackOperation",
    "apply_driver_features",
    "build_async_pipeline_execution_result",
    "build_copy_from_command",
    "build_pipeline_execution_result",
    "build_profile",
    "build_statement_config",
    "build_truncate_command",
    "collect_rows",
    "create_mapped_exception",
    "default_statement_config",
    "driver_profile",
    "execute_with_optional_parameters",
    "execute_with_optional_parameters_async",
    "executemany_or_skip",
    "executemany_or_skip_async",
    "pipeline_supported",
    "resolve_rowcount",
)

TRANSACTION_STATUS_IDLE = 0
TRANSACTION_STATUS_ACTIVE = 1
TRANSACTION_STATUS_INTRANS = 2
TRANSACTION_STATUS_INERROR = 3
TRANSACTION_STATUS_UNKNOWN = 4


class PreparedStackOperation(NamedTuple):
    """Precompiled stack operation metadata for psycopg pipeline execution."""

    operation_index: int
    operation: "StackOperation"
    statement: "SQL"
    sql: str
    parameters: "tuple[Any, ...] | dict[str, Any] | None"


class PipelineCursorEntry(NamedTuple):
    """Cursor pending result data for psycopg pipeline execution."""

    prepared: "PreparedStackOperation"
    cursor: Any


def pipeline_supported() -> bool:
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


def _build_psycopg_parameter_config(
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


def build_profile() -> "DriverParameterProfile":
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


driver_profile = build_profile()


def build_statement_config(*, json_serializer: "Callable[[Any], str] | None" = None) -> "StatementConfig":
    """Construct the psycopg statement configuration with optional JSON codecs."""
    serializer = json_serializer or to_json
    profile = driver_profile
    parameter_config = _build_psycopg_parameter_config(profile, serializer)
    base_config = build_statement_config_from_profile(profile, json_serializer=serializer)
    return base_config.replace(parameter_config=parameter_config)


default_statement_config = build_statement_config()


def apply_driver_features(
    statement_config: "StatementConfig", driver_features: "Mapping[str, Any] | None"
) -> "tuple[StatementConfig, dict[str, Any]]":
    """Apply psycopg driver feature defaults to statement config."""
    features: dict[str, Any] = dict(driver_features) if driver_features else {}
    serializer = features.get("json_serializer", to_json)
    features.setdefault("json_serializer", serializer)
    features.setdefault("enable_pgvector", PGVECTOR_INSTALLED)

    parameter_config = _build_psycopg_parameter_config(driver_profile, serializer)
    statement_config = statement_config.replace(parameter_config=parameter_config)

    return statement_config, features


def collect_rows(fetched_data: "list[Any] | None", description: "list[Any] | None") -> "tuple[list[Any], list[str]]":
    """Collect psycopg rows and column names.

    Args:
        fetched_data: Rows returned from cursor.fetchall().
        description: Cursor description metadata.

    Returns:
        Tuple of (rows, column_names).
    """
    if not description:
        return [], []
    column_names = [col.name for col in description]
    return fetched_data or [], column_names


def execute_with_optional_parameters(cursor: Any, sql: str, parameters: Any) -> None:
    """Execute statement with optional parameters.

    Args:
        cursor: Psycopg cursor object.
        sql: SQL string to execute.
        parameters: Prepared parameters payload.
    """
    if parameters:
        cursor.execute(sql, parameters)
    else:
        cursor.execute(sql)


async def execute_with_optional_parameters_async(cursor: Any, sql: str, parameters: Any) -> None:
    """Execute statement with optional parameters in async mode.

    Args:
        cursor: Psycopg async cursor object.
        sql: SQL string to execute.
        parameters: Prepared parameters payload.
    """
    if parameters:
        await cursor.execute(sql, parameters)
    else:
        await cursor.execute(sql)


def executemany_or_skip(cursor: Any, sql: str, parameters: Any) -> bool:
    """Execute executemany when parameters are provided.

    Args:
        cursor: Psycopg cursor object.
        sql: SQL string to execute.
        parameters: Prepared parameters payload.

    Returns:
        True when executemany was executed.
    """
    if not parameters:
        return False
    cursor.executemany(sql, parameters)
    return True


async def executemany_or_skip_async(cursor: Any, sql: str, parameters: Any) -> bool:
    """Execute executemany when parameters are provided in async mode.

    Args:
        cursor: Psycopg async cursor object.
        sql: SQL string to execute.
        parameters: Prepared parameters payload.

    Returns:
        True when executemany was executed.
    """
    if not parameters:
        return False
    await cursor.executemany(sql, parameters)
    return True


def resolve_rowcount(cursor: Any) -> int:
    """Resolve rowcount from a psycopg cursor.

    Args:
        cursor: Psycopg cursor with optional rowcount metadata.

    Returns:
        Positive rowcount value or 0 when unknown.
    """

    if not has_rowcount(cursor):
        return 0
    rowcount = cursor.rowcount
    if isinstance(rowcount, int) and rowcount > 0:
        return rowcount
    return 0


def build_pipeline_execution_result(statement: "SQL", cursor: Any) -> "ExecutionResult":
    """Build an ExecutionResult for psycopg pipeline execution.

    Args:
        statement: SQL statement executed by the pipeline.
        cursor: Psycopg cursor holding the pipeline result.

    Returns:
        ExecutionResult representing the pipeline operation.
    """

    if statement.returns_rows():
        fetched_data = cursor.fetchall()
        fetched_data, column_names = collect_rows(cast("list[Any] | None", fetched_data), cursor.description)
        return ExecutionResult(
            cursor_result=cursor,
            rowcount_override=None,
            special_data=None,
            selected_data=fetched_data,
            column_names=column_names,
            data_row_count=len(fetched_data),
            statement_count=None,
            successful_statements=None,
            is_script_result=False,
            is_select_result=True,
            is_many_result=False,
            last_inserted_id=None,
        )

    affected_rows = resolve_rowcount(cursor)
    return ExecutionResult(
        cursor_result=cursor,
        rowcount_override=affected_rows,
        special_data=None,
        selected_data=None,
        column_names=None,
        data_row_count=None,
        statement_count=None,
        successful_statements=None,
        is_script_result=False,
        is_select_result=False,
        is_many_result=False,
        last_inserted_id=None,
    )


async def build_async_pipeline_execution_result(statement: "SQL", cursor: Any) -> "ExecutionResult":
    """Build an ExecutionResult for psycopg async pipeline execution.

    Args:
        statement: SQL statement executed by the pipeline.
        cursor: Psycopg cursor holding the pipeline result.

    Returns:
        ExecutionResult representing the pipeline operation.
    """

    if statement.returns_rows():
        fetched_data = await cursor.fetchall()
        fetched_data, column_names = collect_rows(cast("list[Any] | None", fetched_data), cursor.description)
        return ExecutionResult(
            cursor_result=cursor,
            rowcount_override=None,
            special_data=None,
            selected_data=fetched_data,
            column_names=column_names,
            data_row_count=len(fetched_data),
            statement_count=None,
            successful_statements=None,
            is_script_result=False,
            is_select_result=True,
            is_many_result=False,
            last_inserted_id=None,
        )

    affected_rows = resolve_rowcount(cursor)
    return ExecutionResult(
        cursor_result=cursor,
        rowcount_override=affected_rows,
        special_data=None,
        selected_data=None,
        column_names=None,
        data_row_count=None,
        statement_count=None,
        successful_statements=None,
        is_script_result=False,
        is_select_result=False,
        is_many_result=False,
        last_inserted_id=None,
    )


def _create_postgres_error(
    error: Any, code: "str | None", error_class: type[SQLSpecError], description: str
) -> SQLSpecError:
    """Create a SQLSpec exception from a psycopg error.

    Args:
        error: The original psycopg exception
        code: PostgreSQL SQLSTATE error code
        error_class: The SQLSpec exception class to instantiate
        description: Human-readable description of the error type

    Returns:
        A new SQLSpec exception instance with the original as its cause
    """
    msg = f"PostgreSQL {description} [{code}]: {error}" if code else f"PostgreSQL {description}: {error}"
    exc = error_class(msg)
    exc.__cause__ = error
    return exc


def create_mapped_exception(error: Any) -> SQLSpecError:
    """Map psycopg exceptions to SQLSpec exceptions.

    This is a factory function that returns an exception instance rather than
    raising. This pattern is more robust for use in __exit__ handlers and
    avoids issues with exception control flow in different Python versions.

    Args:
        error: The psycopg exception to map

    Returns:
        A SQLSpec exception that wraps the original error
    """
    error_code = error.sqlstate if has_sqlstate(error) and error.sqlstate is not None else None
    if not error_code:
        return _create_postgres_error(error, None, SQLSpecError, "database error")

    if error_code == "23505":
        return _create_postgres_error(error, error_code, UniqueViolationError, "unique constraint violation")
    if error_code == "23503":
        return _create_postgres_error(error, error_code, ForeignKeyViolationError, "foreign key constraint violation")
    if error_code == "23502":
        return _create_postgres_error(error, error_code, NotNullViolationError, "not-null constraint violation")
    if error_code == "23514":
        return _create_postgres_error(error, error_code, CheckViolationError, "check constraint violation")
    if error_code.startswith("23"):
        return _create_postgres_error(error, error_code, IntegrityError, "integrity constraint violation")
    if error_code.startswith("42"):
        return _create_postgres_error(error, error_code, SQLParsingError, "SQL syntax error")
    if error_code.startswith("08"):
        return _create_postgres_error(error, error_code, DatabaseConnectionError, "connection error")
    if error_code.startswith("40"):
        return _create_postgres_error(error, error_code, TransactionError, "transaction error")
    if error_code.startswith("22"):
        return _create_postgres_error(error, error_code, DataError, "data error")
    if error_code.startswith(("53", "54", "55", "57", "58")):
        return _create_postgres_error(error, error_code, OperationalError, "operational error")
    return _create_postgres_error(error, error_code, SQLSpecError, "database error")
