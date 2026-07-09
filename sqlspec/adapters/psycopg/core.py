"""psycopg adapter compiled helpers."""

import contextlib
import datetime
from collections.abc import Sized
from typing import TYPE_CHECKING, Any, Final, NamedTuple, cast
from uuid import uuid4

from typing_extensions import LiteralString

from sqlspec.adapters.psycopg._typing import PsycopgComposed, PsycopgIdentifier, PsycopgSQL
from sqlspec.core import (
    SQL,
    DriverParameterProfile,
    ParameterStyle,
    StatementConfig,
    build_statement_config_from_profile,
)
from sqlspec.core.config_runtime import (
    build_postgres_extension_probe_names,
    resolve_postgres_extension_state,
    resolve_runtime_statement_config,
)
from sqlspec.driver import ExecutionResult, rows_to_dicts
from sqlspec.exceptions import (
    CheckViolationError,
    ConnectionTimeoutError,
    DeadlockError,
    ForeignKeyViolationError,
    IntegrityError,
    NotNullViolationError,
    PermissionDeniedError,
    QueryTimeoutError,
    SerializationConflictError,
    SQLParsingError,
    SQLSpecError,
    UniqueViolationError,
    map_sqlstate_to_exception,
)
from sqlspec.typing import PGVECTOR_INSTALLED
from sqlspec.utils.serializers import from_json, to_json
from sqlspec.utils.text import split_qualified_identifier
from sqlspec.utils.type_converters import build_json_list_converter, build_json_tuple_converter, build_uuid_coercions
from sqlspec.utils.type_guards import has_rowcount, has_sqlstate, resolve_row_format

# Module-level lazy import for psycopg errors (mypyc optimization)
try:
    from psycopg import errors as pg_errors
except ImportError:
    pg_errors = None  # type: ignore[assignment]

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping

    from sqlspec.core import ParameterStyleConfig, StackOperation

__all__ = (
    "PipelineCursorEntry",
    "PreparedStackOperation",
    "PsycopgAsyncStreamSource",
    "PsycopgSyncStreamSource",
    "apply_driver_features",
    "build_async_pipeline_execution_result",
    "build_copy_from_command",
    "build_pipeline_execution_result",
    "build_postgres_extension_probe_names",
    "build_profile",
    "build_statement_config",
    "build_truncate_command",
    "collect_rows",
    "create_mapped_exception",
    "default_statement_config",
    "driver_profile",
    "execute_with_optional_parameters",
    "execute_with_optional_parameters_async",
    "pipeline_supported",
    "resolve_many_rowcount",
    "resolve_postgres_extension_state",
    "resolve_rowcount",
    "resolve_runtime_statement_config",
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
    sql: "LiteralString | PsycopgSQL | PsycopgComposed"
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


def _compose_table_identifier(table: str) -> "PsycopgComposed":
    parts = split_qualified_identifier(table, quote_chars='"', allow_bracket_quotes=False)
    if not parts:
        msg = "Table name must not be empty"
        raise SQLSpecError(msg)
    identifiers = [PsycopgIdentifier(part) for part in parts]
    return PsycopgSQL(".").join(identifiers)


def build_copy_from_command(table: str, columns: "list[str]") -> "PsycopgComposed":
    table_identifier = _compose_table_identifier(table)
    column_sql = PsycopgSQL(", ").join([PsycopgIdentifier(column) for column in columns])
    return PsycopgSQL("COPY {} ({}) FROM STDIN").format(table_identifier, column_sql)


def build_truncate_command(table: str) -> "PsycopgComposed":
    return PsycopgSQL("TRUNCATE TABLE {}").format(_compose_table_identifier(table))


def _identity(value: Any) -> Any:
    return value


def _custom_type_coercions() -> "dict[type, Callable[[Any], Any]]":
    """Return custom type coercions for psycopg."""

    return {
        datetime.datetime: _identity,
        datetime.date: _identity,
        datetime.time: _identity,
        **build_uuid_coercions(native=True),
    }


def _parameter_config(
    profile: "DriverParameterProfile", serializer: "Callable[[Any], str]", deserializer: "Callable[[str], Any]"
) -> "ParameterStyleConfig":
    """Construct parameter configuration with shared JSON serializer support.

    Args:
        profile: Driver parameter profile to extend.
        serializer: JSON serializer for parameter coercion.
        deserializer: JSON deserializer for result coercion.

    Returns:
        ParameterStyleConfig with updated type coercions.
    """

    base_config = build_statement_config_from_profile(
        profile, json_serializer=serializer, json_deserializer=deserializer
    ).parameter_config

    updated_type_map = dict(base_config.type_coercion_map)
    updated_type_map[list] = build_json_list_converter(serializer)
    updated_type_map[tuple] = build_json_tuple_converter(serializer)

    return base_config.replace(type_coercion_map=updated_type_map)


def build_profile() -> "DriverParameterProfile":
    """Create the psycopg driver parameter profile."""

    return DriverParameterProfile(
        name="Psycopg",
        default_style=ParameterStyle.NUMERIC,
        supported_styles={ParameterStyle.NUMERIC, ParameterStyle.NAMED_COLON},
        default_execution_style=ParameterStyle.POSITIONAL_PYFORMAT,
        supported_execution_styles={ParameterStyle.POSITIONAL_PYFORMAT, ParameterStyle.NAMED_PYFORMAT},
        has_native_list_expansion=True,
        preserve_parameter_format=True,
        needs_static_script_compilation=False,
        allow_mixed_parameter_styles=False,
        preserve_original_params_for_many=False,
        json_serializer_strategy="helper",
        custom_type_coercions=_custom_type_coercions(),
        default_dialect="postgres",
    )


driver_profile = build_profile()


def build_statement_config(
    *, json_serializer: "Callable[[Any], str] | None" = None, json_deserializer: "Callable[[str], Any] | None" = None
) -> "StatementConfig":
    """Construct the psycopg statement configuration with optional JSON codecs."""
    serializer = json_serializer or to_json
    deserializer = json_deserializer or from_json
    profile = driver_profile
    parameter_config = _parameter_config(profile, serializer, deserializer)
    base_config = build_statement_config_from_profile(
        profile, json_serializer=serializer, json_deserializer=deserializer
    )
    return base_config.replace(parameter_config=parameter_config)


default_statement_config = build_statement_config()


def apply_driver_features(
    statement_config: "StatementConfig", driver_features: "Mapping[str, Any] | None"
) -> "tuple[StatementConfig, dict[str, Any]]":
    """Apply psycopg driver feature defaults to statement config."""
    features: dict[str, Any] = dict(driver_features) if driver_features else {}
    serializer = features.get("json_serializer", to_json)
    deserializer = features.get("json_deserializer", from_json)
    features.setdefault("json_serializer", serializer)
    features.setdefault("json_deserializer", deserializer)
    features.setdefault("enable_pgvector", PGVECTOR_INSTALLED)
    features.setdefault("enable_paradedb", True)

    parameter_config = _parameter_config(driver_profile, serializer, deserializer)
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


class PsycopgSyncStreamSource:
    """Compiled chunk source streaming dict rows from a psycopg server-side named cursor.

    The server-side cursor is declared inside a stream-owned transaction (a savepoint
    when one is already active, a transaction block under autocommit).
    """

    __slots__ = ("_chunk_size", "_column_names", "_cursor", "_driver", "_parameters", "_sql", "_transaction")

    def __init__(self, driver: Any, sql: str, parameters: Any, chunk_size: int) -> None:
        self._driver = driver
        self._sql = sql
        self._parameters = parameters
        self._chunk_size = chunk_size
        self._cursor: Any = None
        self._transaction: Any = None
        self._column_names: list[str] | None = None

    def start(self) -> None:
        handler = self._driver.handle_database_exceptions()
        with handler:
            transaction = self._driver.connection.transaction()
            transaction.__enter__()
            self._transaction = transaction
            try:
                cursor = self._driver.connection.cursor(name=f"sqlspec_stream_{uuid4().hex}")
                cursor.itersize = self._chunk_size
                execute_with_optional_parameters(cursor, self._sql, self._parameters)
                self._cursor = cursor
            except BaseException as exc:
                self._transaction = None
                with contextlib.suppress(Exception):
                    transaction.__exit__(type(exc), exc, exc.__traceback__)
                raise
        self._driver._check_pending_exception(handler)

    def fetch_chunk(self) -> "list[dict[str, Any]]":
        handler = self._driver.handle_database_exceptions()
        rows: list[Any] = []
        with handler:
            rows = self._cursor.fetchmany(self._chunk_size)
        self._driver._check_pending_exception(handler)
        if not rows:
            return []
        if self._column_names is None:
            self._column_names = [column.name for column in self._cursor.description]
        return rows_to_dicts(rows, self._column_names)

    def close(self, error: bool = False) -> None:
        cursor = self._cursor
        self._cursor = None
        if cursor is not None:
            with contextlib.suppress(Exception):
                cursor.close()
        transaction = self._transaction
        self._transaction = None
        if transaction is not None:
            with contextlib.suppress(Exception):
                if error:
                    transaction.__exit__(RuntimeError, RuntimeError("stream failed"), None)
                else:
                    transaction.__exit__(None, None, None)


class PsycopgAsyncStreamSource:
    """Compiled async chunk source streaming dict rows from a psycopg server-side named cursor.

    The server-side cursor is declared inside a stream-owned transaction (a savepoint
    when one is already active, a transaction block under autocommit).
    """

    __slots__ = ("_chunk_size", "_column_names", "_cursor", "_driver", "_parameters", "_sql", "_transaction")

    def __init__(self, driver: Any, sql: str, parameters: Any, chunk_size: int) -> None:
        self._driver = driver
        self._sql = sql
        self._parameters = parameters
        self._chunk_size = chunk_size
        self._cursor: Any = None
        self._transaction: Any = None
        self._column_names: list[str] | None = None

    async def start(self) -> None:
        handler = self._driver.handle_database_exceptions()
        async with handler:
            transaction = self._driver.connection.transaction()
            await transaction.__aenter__()
            self._transaction = transaction
            try:
                cursor = self._driver.connection.cursor(name=f"sqlspec_stream_{uuid4().hex}")
                cursor.itersize = self._chunk_size
                await execute_with_optional_parameters_async(cursor, self._sql, self._parameters)
                self._cursor = cursor
            except BaseException as exc:
                self._transaction = None
                with contextlib.suppress(Exception):
                    await transaction.__aexit__(type(exc), exc, exc.__traceback__)
                raise
        self._driver._check_pending_exception(handler)

    async def fetch_chunk(self) -> "list[dict[str, Any]]":
        handler = self._driver.handle_database_exceptions()
        rows: list[Any] = []
        async with handler:
            rows = await self._cursor.fetchmany(self._chunk_size)
        self._driver._check_pending_exception(handler)
        if not rows:
            return []
        if self._column_names is None:
            self._column_names = [column.name for column in self._cursor.description]
        return rows_to_dicts(rows, self._column_names)

    async def close(self, error: bool = False) -> None:
        cursor = self._cursor
        self._cursor = None
        if cursor is not None:
            with contextlib.suppress(Exception):
                await cursor.close()
        transaction = self._transaction
        self._transaction = None
        if transaction is not None:
            with contextlib.suppress(Exception):
                if error:
                    await transaction.__aexit__(RuntimeError, RuntimeError("stream failed"), None)
                else:
                    await transaction.__aexit__(None, None, None)


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


def resolve_many_rowcount(cursor: Any, parameters: Any, *, fallback_count: "int | None" = None) -> int:
    """Resolve rowcount for execute_many operations.

    Prefers the driver-provided rowcount when available, with a fallback to
    the number of parameter sets when the driver reports unknown rowcount.

    Args:
        cursor: Psycopg cursor with optional rowcount metadata.
        parameters: Prepared executemany parameter payload.
        fallback_count: Optional precomputed parameter payload size.

    Returns:
        Positive rowcount value, parameter set count, or 0 when unknown.
    """

    try:
        rowcount = cursor.rowcount
    except AttributeError:
        rowcount = None
    if isinstance(rowcount, int) and rowcount > 0:
        return rowcount
    if fallback_count is not None:
        return fallback_count
    if isinstance(parameters, Sized):
        return len(parameters)
    return 0


def build_pipeline_execution_result(
    statement: "SQL", cursor: Any, *, column_name_resolver: "Callable[[Any], list[str]] | None" = None
) -> "ExecutionResult":
    """Build an ExecutionResult for psycopg pipeline execution.

    Args:
        statement: SQL statement executed by the pipeline.
        cursor: Psycopg cursor holding the pipeline result.
        column_name_resolver: Optional cached column-name resolver.

    Returns:
        ExecutionResult representing the pipeline operation.
    """

    if statement.returns_rows():
        fetched_data = cast("list[Any] | None", cursor.fetchall()) or []
        if column_name_resolver is None:
            fetched_data, column_names = collect_rows(fetched_data, cursor.description)
        else:
            column_names = column_name_resolver(cursor.description)
        row_format = resolve_row_format(fetched_data)
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
            row_format=row_format,
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


async def build_async_pipeline_execution_result(
    statement: "SQL", cursor: Any, *, column_name_resolver: "Callable[[Any], list[str]] | None" = None
) -> "ExecutionResult":
    """Build an ExecutionResult for psycopg async pipeline execution.

    Args:
        statement: SQL statement executed by the pipeline.
        cursor: Psycopg cursor holding the pipeline result.
        column_name_resolver: Optional cached column-name resolver.

    Returns:
        ExecutionResult representing the pipeline operation.
    """

    if statement.returns_rows():
        fetched_data = cast("list[Any] | None", await cursor.fetchall()) or []
        if column_name_resolver is None:
            fetched_data, column_names = collect_rows(fetched_data, cursor.description)
        else:
            column_names = column_name_resolver(cursor.description)
        row_format = resolve_row_format(fetched_data)
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
            row_format=row_format,
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


_EXCEPTION_MAPPING: Final[dict[type[Any], tuple[str, type[SQLSpecError], str]]] = {}
_EXCEPTION_MAPPING_CACHE: Final[dict[type[Any], tuple[str, type[SQLSpecError], str]]] = {}


def _register_exception_mappings() -> None:
    if pg_errors is None:
        return

    _EXCEPTION_MAPPING.update({
        pg_errors.UniqueViolation: ("23505", UniqueViolationError, "unique constraint violation"),
        pg_errors.ForeignKeyViolation: ("23503", ForeignKeyViolationError, "foreign key constraint violation"),
        pg_errors.NotNullViolation: ("23502", NotNullViolationError, "not-null constraint violation"),
        pg_errors.CheckViolation: ("23514", CheckViolationError, "check constraint violation"),
        pg_errors.IntegrityError: ("23000", IntegrityError, "integrity constraint violation"),
        pg_errors.DeadlockDetected: ("40P01", DeadlockError, "deadlock detected"),
        pg_errors.SerializationFailure: ("40001", SerializationConflictError, "serialization failure"),
        pg_errors.QueryCanceled: ("57014", QueryTimeoutError, "query canceled"),
        pg_errors.InsufficientPrivilege: ("42501", PermissionDeniedError, "insufficient privilege"),
        pg_errors.SyntaxError: ("42601", SQLParsingError, "SQL syntax error"),
    })

    admin_shutdown = getattr(pg_errors, "AdminShutdown", None)
    if isinstance(admin_shutdown, type):
        _EXCEPTION_MAPPING[admin_shutdown] = ("57P01", ConnectionTimeoutError, "admin shutdown")

    cannot_connect_now = getattr(pg_errors, "CannotConnectNow", None)
    if isinstance(cannot_connect_now, type):
        _EXCEPTION_MAPPING[cannot_connect_now] = ("57P03", ConnectionTimeoutError, "cannot connect now")


def _resolve_exception_mapping(error_type: type[Any]) -> "tuple[str, type[SQLSpecError], str] | None":
    mapped_error = _EXCEPTION_MAPPING_CACHE.get(error_type)
    if mapped_error is not None:
        return mapped_error

    for base_type in error_type.__mro__[1:]:
        mapped_error = _EXCEPTION_MAPPING.get(base_type)
        if mapped_error is not None:
            _EXCEPTION_MAPPING_CACHE[error_type] = mapped_error
            return mapped_error
    return None


_register_exception_mappings()


def create_mapped_exception(error: Any, *, logger: Any | None = None) -> SQLSpecError:
    """Map psycopg exceptions to SQLSpec exceptions.

    This is a factory function that returns an exception instance rather than
    raising. This pattern is more robust for use in __exit__ handlers and
    avoids issues with exception control flow in different Python versions.

    Mapping priority:
        1. Native psycopg exception types via adapter-local dispatch table
        2. SQLSTATE code via centralized utility
        3. Generic SQLSpecError fallback

    Args:
        error: The psycopg exception to map
        logger: Optional logger accepted for adapter signature parity.

    Returns:
        A SQLSpec exception that wraps the original error
    """
    del logger
    error_type = type(error)
    mapped_error = _EXCEPTION_MAPPING.get(error_type)
    if mapped_error is None:
        mapped_error = _resolve_exception_mapping(error_type)
    if mapped_error is not None:
        mapped_error_code, error_class, description = mapped_error
        return _create_postgres_error(error, mapped_error_code, error_class, description)

    # Priority 2: Fall back to SQLSTATE code mapping using centralized utility
    sqlstate_attr = error.sqlstate if has_sqlstate(error) else None
    error_code = sqlstate_attr if sqlstate_attr is not None else None
    if error_code:
        exc_class = map_sqlstate_to_exception(error_code)
        if exc_class:
            return _create_postgres_error(error, error_code, exc_class, "database error")

    # Priority 3: Default fallback
    return _create_postgres_error(error, error_code, SQLSpecError, "database error")
